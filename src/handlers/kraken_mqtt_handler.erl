%%%-------------------------------------------------------------------
%% @doc MQTT Connection Handler
%% Handles MQTT client connections, authenticates with Titus,
%% and bridges to EMQX for pub/sub.
%% Mirrors kraken_ws_handler.erl functionality for MQTT protocol.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_mqtt_handler).
-behaviour(ranch_protocol).
-behaviour(gen_server).

%% Ranch callbacks
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Rate limiting defaults
-define(DEFAULT_RATE_LIMIT, 50).
-define(MAX_MESSAGE_SIZE, 921600).  %% 900KB flat platform ceiling (all plans)

%% Connection state
-record(state, {
    socket :: gen_tcp:socket(),
    transport :: module(),
    buffer = <<>> :: binary(),
    authenticated = false :: boolean(),
    actor_token_id :: binary() | undefined,
    connection_id :: binary() | undefined,
    organization_id :: binary() | undefined,
    project_id :: binary() | undefined,
    actor_type :: binary() | undefined,
    allowed_topics = [] :: list(),
    apps = [] :: list(),
    mqtt_client :: pid() | undefined,
    kraken_store :: enabled | undefined,  %% Firestore writer status (centralized gen_server)
    keep_alive = 60 :: non_neg_integer(),
    keep_alive_timer :: reference() | undefined,
    %% Rate limiting
    rate_limit = ?DEFAULT_RATE_LIMIT :: non_neg_integer(),
    msg_count = 0 :: non_neg_integer(),
    rate_limit_second = 0 :: non_neg_integer()
}).

%%====================================================================
%% Ranch Protocol Callbacks
%%====================================================================

start_link(Ref, Transport, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

%%====================================================================
%% gen_server Callbacks
%%====================================================================

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, raw}, binary]),
    kraken_log:info("[MQTT] Connection opened~n", []),
    ConnectionId = generate_connection_id(),
    %% Start per-connection Firestore writer (lazy connect)
    {ok, WriterPid} = kraken_store:start_writer(),
    gen_server:enter_loop(?MODULE, [], #state{
        socket = Socket,
        transport = Transport,
        connection_id = ConnectionId,
        kraken_store = WriterPid
    }).

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle incoming TCP data
handle_info({tcp, Socket, Data}, #state{socket = Socket, transport = Transport, buffer = Buffer} = State) ->
    NewBuffer = <<Buffer/binary, Data/binary>>,
    case process_buffer(NewBuffer, State) of
        {ok, State1, Rest} ->
            ok = Transport:setopts(Socket, [{active, once}]),
            {noreply, State1#state{buffer = Rest}};
        {error, Reason, State1} ->
            kraken_log:error("[MQTT] Error: ~p~n", [Reason]),
            {stop, normal, State1};
        {stop, State1} ->
            {stop, normal, State1}
    end;

%% Handle TCP close
handle_info({tcp_closed, _Socket}, State) ->
    kraken_log:info("[MQTT] Connection closed~n", []),
    {stop, normal, State};

%% Handle TCP error
handle_info({tcp_error, _Socket, Reason}, State) ->
    kraken_log:error("[MQTT] TCP error: ~p~n", [Reason]),
    {stop, normal, State};

%% Handle keep-alive timeout
handle_info(keep_alive_timeout, State) ->
    kraken_log:info("[MQTT] Keep-alive timeout, closing connection~n", []),
    {stop, normal, State};

%% Handle MQTT messages from EMQX bridge
handle_info({mqtt_message, Topic, Payload}, #state{socket = Socket, transport = Transport} = State) ->
    %% Forward message to MQTT client
    Packet = kraken_mqtt_protocol:encode_publish(Topic, Payload, 0, undefined),
    Transport:send(Socket, Packet),
    {noreply, State};

%% Handle MQTT message with QoS
handle_info({mqtt_message, Topic, Payload, QoS, PacketId}, #state{socket = Socket, transport = Transport} = State) ->
    Packet = kraken_mqtt_protocol:encode_publish(Topic, Payload, QoS, PacketId),
    Transport:send(Socket, Packet),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{mqtt_client = MqttClient, socket = Socket, transport = Transport,
                          kraken_store = FirestoreWriter}) ->
    %% Disconnect from EMQX
    case MqttClient of
        undefined -> ok;
        Pid -> kraken_broker:disconnect(Pid)
    end,
    %% Stop Firestore writer process
    kraken_store:stop_writer(FirestoreWriter),
    %% Close socket
    Transport:close(Socket),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Process buffer, decode packets
process_buffer(Buffer, State) ->
    case kraken_mqtt_protocol:decode(Buffer) of
        {ok, Packet, Rest} ->
            case handle_packet(Packet, State) of
                {ok, State1} ->
                    process_buffer(Rest, State1);
                {error, Reason, State1} ->
                    {error, Reason, State1};
                {stop, State1} ->
                    {stop, State1}
            end;
        incomplete ->
            {ok, State, Buffer};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% Handle CONNECT packet
handle_packet({connect, ConnectData}, #state{socket = Socket, transport = Transport} = State) ->
    #{username := Username, password := Password, keep_alive := KeepAlive, client_id := ClientId} = ConnectData,

    %% Use password as actor token (username can be anything or empty)
    Token = case Password of
        undefined -> Username;  %% Fallback to username if no password
        _ -> Password
    end,

    case Token of
        undefined ->
            %% No credentials
            Connack = kraken_mqtt_protocol:encode_connack(false, bad_credentials),
            Transport:send(Socket, Connack),
            {error, no_credentials, State};
        _ ->
            %% Authenticate with Titus
            case kraken_auth:validate_token(Token) of
                {ok, AuthData} ->
                    %% Extract auth data
                    ActorTokenId = maps:get(<<"actor_token_id">>, AuthData),
                    OrganizationId = maps:get(<<"organization_id">>, AuthData),
                    ProjectId = maps:get(<<"project_id">>, AuthData),
                    ActorType = maps:get(<<"actor_type">>, AuthData, <<"device">>),
                    Apps = maps:get(<<"apps">>, AuthData, []),

                    %% Flatten topics from all apps for ACL checks
                    AllowedTopics = kraken_auth:flatten_topics(Apps),

                    %% Connect to EMQX
                    {ok, MqttClient} = kraken_broker:connect(),

                    %% Set up keep-alive timer (1.5x client's keep-alive)
                    KATimeout = case KeepAlive of
                        0 -> infinity;
                        _ -> KeepAlive * 1500  %% 1.5x in milliseconds
                    end,
                    Timer = case KATimeout of
                        infinity -> undefined;
                        _ -> erlang:send_after(KATimeout, self(), keep_alive_timeout)
                    end,

                    %% Send CONNACK success
                    Connack = kraken_mqtt_protocol:encode_connack(false, accepted),
                    Transport:send(Socket, Connack),

                    {ok, State#state{
                        authenticated = true,
                        actor_token_id = ActorTokenId,
                        organization_id = OrganizationId,
                        project_id = ProjectId,
                        actor_type = ActorType,
                        allowed_topics = AllowedTopics,
                        apps = Apps,
                        mqtt_client = MqttClient,
                        keep_alive = KeepAlive,
                        keep_alive_timer = Timer
                    }};
                {error, Reason} ->
                    kraken_log:error("[MQTT] Auth failed: ~p~n", [Reason]),
                    Connack = kraken_mqtt_protocol:encode_connack(false, not_authorized),
                    Transport:send(Socket, Connack),
                    {error, auth_failed, State}
            end
    end;

%% Handle SUBSCRIBE packet (must be authenticated)
handle_packet({subscribe, PacketId, Topics}, #state{authenticated = true, socket = Socket,
                                                      transport = Transport, mqtt_client = MqttClient,
                                                      allowed_topics = AllowedTopics,
                                                      actor_token_id = ActorTokenId} = State) ->
    %% Process each topic subscription
    ReturnCodes = lists:map(fun({Topic, RequestedQoS}) ->
        case kraken_acl:can_subscribe(Topic, AllowedTopics) of
            true ->
                %% Unified resolution — identical to the WS handler so MQTT
                %% and WS clients on the same wildcard rule share one topic
                %% (previously MQTT used the bare pattern and split-brained)
                MqttTopic = case kraken_topics:resolve(Topic, AllowedTopics) of
                    {exact, IT, _RId, _AId} -> IT;
                    {wildcard, FT, _AId, _Rule} ->
                        kraken_log:info("[MQTT] Wildcard fallback subscribe: ~s -> ~s (actor ~s)",
                            [Topic, FT, ActorTokenId]),
                        FT;
                    no_match ->
                        kraken_topics:fallback_topic(<<"unscoped">>, Topic)
                end,
                GrantedQoS = min(RequestedQoS, 2),
                ok = kraken_broker:subscribe(MqttClient, MqttTopic, Topic, self(), GrantedQoS),
                kraken_subscriptions:track(ActorTokenId, Topic, subscribe),
                GrantedQoS;
            false ->
                kraken_log:info("[MQTT] SUBACK failure for ~s (actor ~s): not authorized or room not configured",
                    [Topic, ActorTokenId]),
                failure
        end
    end, Topics),

    Suback = kraken_mqtt_protocol:encode_suback(PacketId, ReturnCodes),
    Transport:send(Socket, Suback),
    {ok, State};

%% Handle UNSUBSCRIBE packet
handle_packet({unsubscribe, PacketId, Topics}, #state{authenticated = true, socket = Socket,
                                                        transport = Transport, mqtt_client = MqttClient,
                                                        allowed_topics = AllowedTopics,
                                                        actor_token_id = ActorTokenId} = State) ->
    lists:foreach(fun(Topic) ->
        {InternalTopic, _RoomId} = find_topic_info(Topic, AllowedTopics),
        MqttTopic = case InternalTopic of
            undefined -> Topic;
            _ -> InternalTopic
        end,
        ok = kraken_broker:unsubscribe(MqttClient, MqttTopic),
        kraken_subscriptions:track(ActorTokenId, Topic, unsubscribe),
        ok
    end, Topics),

    Unsuback = kraken_mqtt_protocol:encode_unsuback(PacketId),
    Transport:send(Socket, Unsuback),
    {ok, State};

%% Handle PUBLISH packet
handle_packet({publish, PublishData}, #state{authenticated = true, socket = Socket,
                                              transport = Transport, mqtt_client = MqttClient,
                                              allowed_topics = AllowedTopics, apps = Apps,
                                              actor_token_id = ActorTokenId, connection_id = ConnectionId,
                                              organization_id = OrganizationId, project_id = ProjectId,
                                              kraken_store = FirestoreWriter} = State) ->
    #{topic := Topic, payload := Payload, qos := QoS, packet_id := PacketId} = PublishData,

    %% Check rate limit
    case check_rate_limit(State) of
        {error, rate_limited, State1} ->
            %% For MQTT, we can't send a custom error, just drop the message
            %% Send appropriate ack to avoid client retries
            case QoS of
                0 -> ok;
                1 ->
                    Puback = kraken_mqtt_protocol:encode_puback(PacketId),
                    Transport:send(Socket, Puback);
                2 ->
                    Pubrec = kraken_mqtt_protocol:encode_pubrec(PacketId),
                    Transport:send(Socket, Pubrec)
            end,
            {ok, State1};
        {ok, State1} ->
            case kraken_acl:can_publish(Topic, AllowedTopics) of
                true ->
                    %% Unified resolution — same base topic as WS publishers
                    {MqttTopic, InternalTopic, RoomId, AppId} =
                        case kraken_topics:resolve(Topic, AllowedTopics) of
                            {exact, IT, RId, AId} -> {IT, IT, RId, AId};
                            {wildcard, FT, AId, _Rule} ->
                                kraken_log:info("[MQTT] Wildcard fallback publish: ~s -> ~s (actor ~s)",
                                    [Topic, FT, ActorTokenId]),
                                {FT, undefined, undefined, AId};
                            no_match ->
                                FT0 = kraken_topics:fallback_topic(<<"unscoped">>, Topic),
                                {FT0, undefined, undefined, <<"unscoped">>}
                        end,
                    _ = Apps,

                    %% Log to Firestore if enabled
                    LogContext = #{
                        organization_id => OrganizationId,
                        project_id => ProjectId,
                        app_id => AppId,
                        room_id => RoomId
                    },
                    maybe_record_message(FirestoreWriter, Topic, InternalTopic, Payload, LogContext, ActorTokenId),

                    %% Publish to EMQX with QoS
                    ok = kraken_broker:publish(MqttClient, MqttTopic, Payload, ConnectionId, QoS),

                    %% Send QoS acknowledgment
                    case QoS of
                        0 -> ok;
                        1 ->
                            Puback = kraken_mqtt_protocol:encode_puback(PacketId),
                            Transport:send(Socket, Puback);
                        2 ->
                            Pubrec = kraken_mqtt_protocol:encode_pubrec(PacketId),
                            Transport:send(Socket, Pubrec)
                    end,

                    {ok, State1};
                false ->
                    %% MQTT 3.1.1 has no publish NACK. Log loudly so denied
                    %% publishes stop being invisible; still ack QoS>0 to
                    %% avoid client retry storms (documented protocol gap).
                    kraken_log:info("[MQTT] Denied publish to ~s dropped (actor ~s): not authorized or room not configured",
                        [Topic, ActorTokenId]),
                    case QoS of
                        0 -> ok;
                        1 ->
                            Puback = kraken_mqtt_protocol:encode_puback(PacketId),
                            Transport:send(Socket, Puback);
                        2 ->
                            Pubrec = kraken_mqtt_protocol:encode_pubrec(PacketId),
                            Transport:send(Socket, Pubrec)
                    end,
                    {ok, State1}
            end
    end;

%% Handle PUBACK (QoS 1 acknowledgment from client)
handle_packet({puback, _PacketId}, State) ->
    %% Client acknowledged our publish, nothing to do
    {ok, State};

%% Handle PUBREC (QoS 2 step 1 - client acknowledges our publish)
handle_packet({pubrec, PacketId}, #state{socket = Socket, transport = Transport} = State) ->
    %% Respond with PUBREL
    Pubrel = kraken_mqtt_protocol:encode_pubrel(PacketId),
    Transport:send(Socket, Pubrel),
    {ok, State};

%% Handle PUBREL (QoS 2 step 2 - client releases after our PUBREC)
handle_packet({pubrel, PacketId}, #state{socket = Socket, transport = Transport} = State) ->
    %% Respond with PUBCOMP to complete QoS 2 handshake
    Pubcomp = kraken_mqtt_protocol:encode_pubcomp(PacketId),
    Transport:send(Socket, Pubcomp),
    {ok, State};

%% Handle PUBCOMP (QoS 2 step 3 - client confirms completion)
handle_packet({pubcomp, _PacketId}, State) ->
    %% QoS 2 handshake complete, nothing to do
    {ok, State};

%% Handle PINGREQ
handle_packet(pingreq, #state{socket = Socket, transport = Transport,
                               keep_alive = KeepAlive, keep_alive_timer = OldTimer} = State) ->
    %% Reset keep-alive timer
    case OldTimer of
        undefined -> ok;
        _ -> erlang:cancel_timer(OldTimer)
    end,
    NewTimer = case KeepAlive of
        0 -> undefined;
        _ -> erlang:send_after(KeepAlive * 1500, self(), keep_alive_timeout)
    end,

    %% Send PINGRESP
    Pingresp = kraken_mqtt_protocol:encode_pingresp(),
    Transport:send(Socket, Pingresp),
    {ok, State#state{keep_alive_timer = NewTimer}};

%% Handle DISCONNECT
handle_packet(disconnect, State) ->
    kraken_log:info("[MQTT] Client disconnected~n", []),
    {stop, State};

%% Handle unauthenticated packets (except CONNECT)
handle_packet(_Packet, #state{authenticated = false} = State) ->
    {error, not_authenticated, State}.

%%====================================================================
%% Helper Functions
%%====================================================================

generate_connection_id() ->
    list_to_binary(io_lib:format("mqtt-~s", [
        binary_to_list(base64:encode(crypto:strong_rand_bytes(12)))
    ])).

%% Check and update rate limit counter
check_rate_limit(State) ->
    CurrentSecond = erlang:system_time(second),
    RateLimitSecond = State#state.rate_limit_second,
    MsgCount = State#state.msg_count,
    RateLimit = State#state.rate_limit,

    case CurrentSecond of
        RateLimitSecond ->
            case MsgCount >= RateLimit of
                true ->
                    {error, rate_limited, State};
                false ->
                    {ok, State#state{msg_count = MsgCount + 1}}
            end;
        _ ->
            {ok, State#state{msg_count = 1, rate_limit_second = CurrentSecond}}
    end.

%% Find internal topic and room_id from allowed topics
find_topic_info(Pattern, AllowedTopics) ->
    find_topic_info_loop(Pattern, AllowedTopics).

find_topic_info_loop(_Pattern, []) ->
    {undefined, undefined};
find_topic_info_loop(Pattern, [TopicInfo | Rest]) when is_map(TopicInfo) ->
    TopicPattern = maps:get(<<"pattern">>, TopicInfo, undefined),
    case TopicPattern of
        Pattern ->
            InternalTopic = maps:get(<<"topic">>, TopicInfo, undefined),
            RoomId = maps:get(<<"roomId">>, TopicInfo, undefined),
            {InternalTopic, RoomId};
        _ ->
            find_topic_info_loop(Pattern, Rest)
    end;
find_topic_info_loop(Pattern, [_ | Rest]) ->
    find_topic_info_loop(Pattern, Rest).

notify_usage(undefined, _Bytes) -> ok;
notify_usage(ProjectId, Bytes) ->
    catch kraken_usage:increment(ProjectId, 1, Bytes).

%% Record message to Firestore if enabled
maybe_record_message(FirestoreWriter, Pattern, InternalTopic, Data, Context, ActorTokenId) ->
    %% Always track usage regardless of whether message recording is enabled
    ProjectId = maps:get(project_id, Context, undefined),
    PayloadBytes = byte_size(jsx:encode(Data)),
    notify_usage(ProjectId, PayloadBytes),

    RecordMessages = application:get_env(kraken, record_messages, false),
    ShouldRecord = case RecordMessages of
        true -> true;
        "true" -> true;
        <<"true">> -> true;
        _ -> false
    end,
    case ShouldRecord of
        true ->
            MessageId = generate_uuid(),
            Timestamp = erlang:system_time(millisecond),
            %% Pack only when recording; same options as kraken_ws_handler so stored
            %% bytes are identical regardless of ingress protocol
            PackedPayload = iolist_to_binary(msgpack:pack(Data, [{pack_str, from_binary}])),
            kraken_store:log_message(FirestoreWriter, MessageId, Context, InternalTopic, Pattern, ActorTokenId, PackedPayload, Timestamp),
            {ok, MessageId};
        false ->
            {skip, undefined}
    end.

%% Generate a UUID v4
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:strong_rand_bytes(16),
    C2 = (C band 16#0fff) bor 16#4000,
    D2 = (D band 16#3fff) bor 16#8000,
    list_to_binary(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                                  [A, B, C2, D2, E])).
