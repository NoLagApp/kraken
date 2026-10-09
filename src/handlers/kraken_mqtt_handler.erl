%%%-------------------------------------------------------------------
%% @doc MQTT Connection Handler
%% Handles MQTT client connections, authenticates with Titus,
%% and bridges to EMQX for pub/sub.
%% Mirrors kraken_ws_handler.erl functionality for MQTT protocol.
%%
%% Payload rule (see docs/PROTOCOL.md, "MQTT ingress"):
%%  - an MQTT PUBLISH payload is published as one opaque binary, so
%%    WebSocket subscribers receive it as `data' (a string when the bytes
%%    are valid UTF-8, otherwise binary);
%%  - a delivery to an MQTT subscriber carries the message data itself:
%%    a binary is sent as its raw bytes, anything else as its MessagePack
%%    encoding. The no-echo and delivery-tracking wrappers are removed, and
%%    messages this connection published are not delivered back to it.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_mqtt_handler).
-behaviour(ranch_protocol).
-behaviour(gen_server).

%% Ranch callbacks. cowboy 2.10 pins ranch 1.8, which starts a connection
%% with start_link(Ref, Socket, Transport, Opts); ranch 2.x drops the socket
%% argument. Both arities are exported so the listener works with either.
-export([start_link/3, start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Pure helpers, exported for unit tests
-export([subscription_topic/2, display_topic/2, delivery_payload/2]).

%% Rate limiting defaults
-define(DEFAULT_RATE_LIMIT, 50).
-define(MAX_MESSAGE_SIZE, 921600).  %% 900KB flat platform ceiling (all plans)
%% Largest MQTT packet the connection will buffer: the same 1MB hard cap the
%% WebSocket listener puts on a frame. Without it a client could announce a
%% packet of up to 256MB and have it buffered whole before it is rejected.
-define(MAX_PACKET_SIZE, 1048576).
%% Deliveries to MQTT subscribers are sent at QoS 0 and SUBACK grants QoS 0:
%% the handler keeps no session state to retransmit from, so it does not
%% promise more.
-define(DELIVERY_QOS, 0).

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
    %% Client topic filter => broker topic, for every live subscription
    subscriptions = #{} :: #{binary() => binary()},
    %% Rate limiting
    rate_limit = ?DEFAULT_RATE_LIMIT :: non_neg_integer(),
    msg_count = 0 :: non_neg_integer(),
    rate_limit_second = 0 :: non_neg_integer(),
    %% Same session rules as the WebSocket path: client-token (JWT) expiry,
    %% periodic revalidation (a revoked token is disconnected) and the
    %% per-organization connection limit
    max_connections = unlimited :: non_neg_integer() | unlimited,
    revalidation_in_progress = false :: boolean()
}).

%% Revalidate the token this often, like kraken_ws_handler
-define(REVALIDATION_INTERVAL_MS, 600000).

%%====================================================================
%% Ranch Protocol Callbacks
%%====================================================================

start_link(Ref, _Socket, Transport, Opts) ->
    start_link(Ref, Transport, Opts).

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
        {ok, State1, Rest} when byte_size(Rest) > ?MAX_PACKET_SIZE ->
            kraken_log:info("[MQTT] Packet over ~B bytes, closing connection (actor ~s)~n",
                [?MAX_PACKET_SIZE, State1#state.actor_token_id]),
            {stop, normal, State1};
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

%% Handle keep-alive timeout (a timer that was since replaced is ignored)
handle_info({timeout, Timer, keep_alive}, #state{keep_alive_timer = Timer} = State) ->
    kraken_log:info("[MQTT] Keep-alive timeout, closing connection~n", []),
    {stop, normal, State};

%% The broker backends announce each subscription's topic mapping. This
%% handler records the mapping itself when it subscribes, so the
%% announcement carries nothing new.
handle_info({store_topic_mapping, _BrokerTopic, _Filter}, State) ->
    {noreply, State};

%% A message from the broker backend for one of this connection's
%% subscriptions
handle_info({mqtt_publish, #{topic := Topic, payload := Packed} = Msg},
            #state{socket = Socket, transport = Transport, connection_id = ConnectionId,
                   subscriptions = Subscriptions} = State) ->
    %% The syn backend reports a wildcard match under the subscription
    %% pattern and carries the published topic as source_topic; an external
    %% MQTT broker always reports the published topic.
    PublishedTopic = maps:get(source_topic, Msg, Topic),
    case display_topic(PublishedTopic, Subscriptions) of
        undefined ->
            %% No live subscription matches, e.g. the message was already
            %% queued when the client unsubscribed
            {noreply, State};
        DisplayTopic ->
            case delivery_payload(Packed, ConnectionId) of
                drop ->
                    {noreply, State};
                {ok, Bytes} ->
                    Packet = kraken_mqtt_protocol:encode_publish(DisplayTopic, Bytes, ?DELIVERY_QOS, undefined),
                    Transport:send(Socket, Packet),
                    {noreply, State}
            end
    end;

%% A client token (JWT) reached its expiry: MQTT 3.1.1 has no disconnect
%% reason, so the connection is closed (the WebSocket path closes with 4003)
handle_info(token_expired, #state{actor_token_id = ActorTokenId} = State) ->
    kraken_log:info("[MQTT] Client token expired for actor ~s, closing~n", [ActorTokenId]),
    {stop, normal, State};

%% Periodic revalidation, as on the WebSocket path: a revoked token is
%% disconnected, a changed grant set applies to new subscribes and publishes
handle_info(revalidate, #state{actor_token_id = ActorTokenId,
                               revalidation_in_progress = false} = State) ->
    Self = self(),
    spawn(fun() ->
        Self ! case kraken_auth:revalidate_token(ActorTokenId) of
            {ok, AuthData} -> {revalidation_success, AuthData};
            {error, Reason} -> {revalidation_failed, Reason};
            {retry, Reason} -> {revalidation_retry, Reason}
        end
    end),
    {noreply, State#state{revalidation_in_progress = true}};
handle_info(revalidate, State) ->
    {noreply, State};

handle_info({revalidation_success, AuthData}, #state{organization_id = OrgId} = State) ->
    erlang:send_after(?REVALIDATION_INTERVAL_MS, self(), revalidate),
    MaxConn = maps:get(max_connections, AuthData, State#state.max_connections),
    State1 = State#state{
        allowed_topics = maps:get(allowed_topics, AuthData, State#state.allowed_topics),
        apps = maps:get(apps, AuthData, State#state.apps),
        max_connections = MaxConn,
        revalidation_in_progress = false
    },
    %% The limit may have been lowered; this connection is already counted
    case over_connection_limit(OrgId, MaxConn, 1) of
        false ->
            {noreply, State1};
        true ->
            kraken_log:info("[MQTT] Org ~s over its connection limit after revalidation, closing~n", [OrgId]),
            {stop, normal, State1}
    end;

handle_info({revalidation_failed, Reason}, #state{actor_token_id = ActorTokenId} = State) ->
    kraken_log:info("[MQTT] Revalidation failed for ~s: ~p, closing~n", [ActorTokenId, Reason]),
    {stop, normal, State};

handle_info({revalidation_retry, _Reason}, State) ->
    erlang:send_after(?REVALIDATION_INTERVAL_MS, self(), revalidate),
    {noreply, State#state{revalidation_in_progress = false}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{mqtt_client = MqttClient, socket = Socket, transport = Transport,
                          kraken_store = FirestoreWriter}) ->
    %% Disconnect from EMQX. The syn backend needs no cleanup: syn drops a
    %% process from every group it joined when the process exits.
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
                    process_buffer(Rest, reset_keep_alive(State1));
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

%% Handle a second CONNECT on an established connection: a protocol
%% violation, answered by closing the connection
handle_packet({connect, _ConnectData}, #state{authenticated = true} = State) ->
    {error, duplicate_connect, State};

%% Handle CONNECT packet
handle_packet({connect, ConnectData}, #state{socket = Socket, transport = Transport} = State) ->
    #{username := Username, password := Password, keep_alive := KeepAlive} = ConnectData,

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
            %% Authenticate through the configured auth backend. The result
            %% map has atom keys (kraken_auth:build_auth_data/1) and already
            %% carries the flattened allowed_topics.
            case validate_session(Token) of
                {ok, AuthData} ->
                    OrgId = maps:get(organization_id, AuthData, undefined),
                    ActorTokenId = maps:get(actor_token_id, AuthData),
                    %% Counts against the organization's connection limit,
                    %% exactly like a WebSocket connection
                    syn:join(kraken_connections, {org, OrgId}, self(),
                             #{actor_token_id => ActorTokenId}),
                    schedule_token_expiry(maps:get(auth_expires_at, AuthData, undefined)),
                    erlang:send_after(?REVALIDATION_INTERVAL_MS, self(), revalidate),

                    %% Connect to the broker backend
                    {ok, MqttClient} = kraken_broker:connect(),

                    %% Send CONNACK success
                    Connack = kraken_mqtt_protocol:encode_connack(false, accepted),
                    Transport:send(Socket, Connack),

                    %% The keep-alive timer starts once process_buffer/2 sees
                    %% this packet handled (reset_keep_alive/1)
                    {ok, State#state{
                        authenticated = true,
                        actor_token_id = ActorTokenId,
                        organization_id = OrgId,
                        max_connections = maps:get(max_connections, AuthData, unlimited),
                        project_id = maps:get(project_id, AuthData, undefined),
                        actor_type = maps:get(actor_type, AuthData, <<"device">>),
                        allowed_topics = maps:get(allowed_topics, AuthData, []),
                        apps = maps:get(apps, AuthData, []),
                        mqtt_client = MqttClient,
                        keep_alive = KeepAlive
                    }};
                {error, connection_limit_reached} ->
                    kraken_log:info("[MQTT] Connection limit reached, refusing CONNECT~n", []),
                    Connack = kraken_mqtt_protocol:encode_connack(false, server_unavailable),
                    Transport:send(Socket, Connack),
                    {error, connection_limit_reached, State};
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
                                                      actor_token_id = ActorTokenId,
                                                      subscriptions = Subscriptions0} = State) ->
    %% Process each topic subscription
    {ReturnCodes, Subscriptions} = lists:mapfoldl(fun({Filter, RequestedQoS}, Subs) ->
        case kraken_acl:can_subscribe(Filter, AllowedTopics) of
            true ->
                BrokerTopic = subscription_topic(Filter, AllowedTopics),
                kraken_log:info("[MQTT] Subscribe ~s -> ~s (actor ~s)~n",
                    [Filter, BrokerTopic, ActorTokenId]),
                %% Re-subscribing to a filter replaces the old subscription
                case maps:find(Filter, Subs) of
                    {ok, BrokerTopic} -> ok;
                    {ok, OldBrokerTopic} -> leave(MqttClient, OldBrokerTopic, maps:remove(Filter, Subs));
                    error -> ok
                end,
                ok = kraken_broker:subscribe(MqttClient, BrokerTopic, Filter, self(), min(RequestedQoS, 2)),
                kraken_subscriptions:track(ActorTokenId, Filter, subscribe),
                {?DELIVERY_QOS, Subs#{Filter => BrokerTopic}};
            false ->
                kraken_log:info("[MQTT] SUBACK failure for ~s (actor ~s): not authorized or room not configured~n",
                    [Filter, ActorTokenId]),
                {failure, Subs}
        end
    end, Subscriptions0, Topics),

    Suback = kraken_mqtt_protocol:encode_suback(PacketId, ReturnCodes),
    Transport:send(Socket, Suback),
    {ok, State#state{subscriptions = Subscriptions}};

%% Handle UNSUBSCRIBE packet. The broker topic is the one SUBSCRIBE recorded
%% for the filter, so unsubscribing leaves exactly what subscribing joined.
handle_packet({unsubscribe, PacketId, Topics}, #state{authenticated = true, socket = Socket,
                                                        transport = Transport, mqtt_client = MqttClient,
                                                        actor_token_id = ActorTokenId,
                                                        subscriptions = Subscriptions0} = State) ->
    Subscriptions = lists:foldl(fun(Filter, Subs) ->
        case maps:take(Filter, Subs) of
            {BrokerTopic, Subs1} ->
                leave(MqttClient, BrokerTopic, Subs1),
                kraken_subscriptions:track(ActorTokenId, Filter, unsubscribe),
                Subs1;
            error ->
                %% Not subscribed: nothing to leave, UNSUBACK all the same
                Subs
        end
    end, Subscriptions0, Topics),

    Unsuback = kraken_mqtt_protocol:encode_unsuback(PacketId),
    Transport:send(Socket, Unsuback),
    {ok, State#state{subscriptions = Subscriptions}};

%% Handle PUBLISH packet
handle_packet({publish, PublishData}, #state{authenticated = true, mqtt_client = MqttClient,
                                              allowed_topics = AllowedTopics,
                                              actor_token_id = ActorTokenId, connection_id = ConnectionId,
                                              organization_id = OrganizationId, project_id = ProjectId,
                                              kraken_store = FirestoreWriter} = State) ->
    #{topic := Topic, payload := Payload, qos := QoS, packet_id := PacketId} = PublishData,

    %% MQTT 3.1.1 has no publish NACK. Every rejection below is dropped with
    %% a broker-side log line and still acked at QoS>0 so the client does not
    %% retry it forever.
    case check_rate_limit(State) of
        {error, rate_limited, State1} ->
            %% Log once per second, on the first publish over the limit
            case State1#state.msg_count =:= State1#state.rate_limit + 1 of
                true ->
                    kraken_log:info("[MQTT] Rate limit of ~B msg/s reached, dropping publishes for the rest of this second (actor ~s)~n",
                        [State1#state.rate_limit, ActorTokenId]);
                false ->
                    ok
            end,
            ack_publish(QoS, PacketId, State1),
            {ok, State1};
        {ok, State1} when byte_size(Payload) > ?MAX_MESSAGE_SIZE ->
            kraken_log:info("[MQTT] Publish to ~s dropped (actor ~s): payload of ~B bytes is over the ~B byte limit~n",
                [Topic, ActorTokenId, byte_size(Payload), ?MAX_MESSAGE_SIZE]),
            ack_publish(QoS, PacketId, State1),
            {ok, State1};
        {ok, State1} ->
            case kraken_acl:can_publish(Topic, AllowedTopics) of
                true ->
                    %% Unified resolution — same base topic as WS publishers
                    {MqttTopic, InternalTopic, RoomId, AppId} =
                        case kraken_topics:resolve(Topic, AllowedTopics) of
                            {exact, IT, RId, AId} -> {IT, IT, RId, AId};
                            {wildcard, FT, AId, _Rule} ->
                                kraken_log:info("[MQTT] Wildcard fallback publish: ~s -> ~s (actor ~s)~n",
                                    [Topic, FT, ActorTokenId]),
                                {FT, undefined, undefined, AId};
                            no_match ->
                                FT0 = kraken_topics:fallback_topic(<<"unscoped">>, Topic),
                                {FT0, undefined, undefined, <<"unscoped">>}
                        end,

                    %% Log to Firestore if enabled
                    LogContext = #{
                        organization_id => OrganizationId,
                        project_id => ProjectId,
                        app_id => AppId,
                        room_id => RoomId
                    },
                    maybe_record_message(FirestoreWriter, Topic, InternalTopic, Payload, LogContext, ActorTokenId),

                    %% Publish the payload bytes as one opaque binary. Sender =
                    %% this connection, so the broker's no-echo envelope keeps
                    %% the message from being delivered back here.
                    ok = kraken_broker:publish(MqttClient, MqttTopic, Payload, ConnectionId, QoS),

                    ack_publish(QoS, PacketId, State1),
                    {ok, State1};
                false ->
                    kraken_log:info("[MQTT] Denied publish to ~s dropped (actor ~s): not authorized or room not configured~n",
                        [Topic, ActorTokenId]),
                    ack_publish(QoS, PacketId, State1),
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

%% Handle PINGREQ (the keep-alive timer is reset for every packet)
handle_packet(pingreq, #state{socket = Socket, transport = Transport} = State) ->
    Pingresp = kraken_mqtt_protocol:encode_pingresp(),
    Transport:send(Socket, Pingresp),
    {ok, State};

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

%% Validate a CONNECT token with the same session rules as the WebSocket
%% path: an expired client token is refused (the 30 s auth cache can serve
%% an already-expired one), and so is a connection over the organization's
%% limit.
validate_session(Token) ->
    case kraken_auth:validate_token(Token) of
        {ok, AuthData} ->
            ExpiresAt = maps:get(auth_expires_at, AuthData, undefined),
            OrgId = maps:get(organization_id, AuthData, undefined),
            MaxConn = maps:get(max_connections, AuthData, unlimited),
            case is_integer(ExpiresAt) andalso erlang:system_time(second) >= ExpiresAt of
                true ->
                    {error, token_expired};
                false ->
                    case over_connection_limit(OrgId, MaxConn, 0) of
                        true -> {error, connection_limit_reached};
                        false -> {ok, AuthData}
                    end
            end;
        Error ->
            Error
    end.

%% Same count as kraken_ws_handler:check_connection_limit/2. Own is how many
%% of the counted connections are this one (0 before it joins, 1 after).
over_connection_limit(_OrgId, unlimited, _Own) -> false;
over_connection_limit(undefined, _MaxConn, _Own) -> false;
over_connection_limit(OrgId, MaxConn, Own) when is_integer(MaxConn) ->
    Current = length(syn:members(kraken_connections, {org, OrgId})),
    Current - Own >= MaxConn;
over_connection_limit(_OrgId, _MaxConn, _Own) -> false.

schedule_token_expiry(ExpiresAt) when is_integer(ExpiresAt) ->
    Ms = max(0, (ExpiresAt - erlang:system_time(second)) * 1000),
    erlang:send_after(Ms, self(), token_expired),
    ok;
schedule_token_expiry(_) ->
    ok.

generate_connection_id() ->
    list_to_binary(io_lib:format("mqtt-~s", [
        binary_to_list(base64:encode(crypto:strong_rand_bytes(12)))
    ])).

%% Keep-alive: MQTT 3.1.1 closes a connection that sends no control packet
%% for 1.5x its keep-alive, so the timer restarts on every packet, not only
%% on PINGREQ (clients that are busy publishing never need to ping).
reset_keep_alive(#state{authenticated = false} = State) ->
    State;
reset_keep_alive(#state{keep_alive = KeepAlive, keep_alive_timer = OldTimer} = State) ->
    case OldTimer of
        undefined -> ok;
        _ -> erlang:cancel_timer(OldTimer)
    end,
    NewTimer = case KeepAlive of
        0 -> undefined;
        _ -> erlang:start_timer(KeepAlive * 1500, self(), keep_alive)
    end,
    State#state{keep_alive_timer = NewTimer}.

%% Acknowledge a PUBLISH at its QoS
ack_publish(0, _PacketId, _State) ->
    ok;
ack_publish(1, PacketId, #state{socket = Socket, transport = Transport}) ->
    Transport:send(Socket, kraken_mqtt_protocol:encode_puback(PacketId));
ack_publish(2, PacketId, #state{socket = Socket, transport = Transport}) ->
    Transport:send(Socket, kraken_mqtt_protocol:encode_pubrec(PacketId)).

%% Leave a broker topic unless another filter in Remaining still uses it
leave(MqttClient, BrokerTopic, Remaining) ->
    case lists:member(BrokerTopic, maps:values(Remaining)) of
        true -> ok;
        false -> ok = kraken_broker:unsubscribe(MqttClient, BrokerTopic)
    end.

%% Broker topic for an MQTT topic filter.
%%
%% A filter without wildcards resolves exactly like a publish (and like a
%% WebSocket subscribe), so MQTT and WebSocket clients on the same pattern
%% share one broker topic. A filter with + or # has to match the topics that
%% publishers resolve to, which under a wildcard grant are
%% <app_id>/<published topic>; so the filter becomes <app_id>/<filter>.
%% kraken_topics:resolve/2 alone would map a filter that equals a rule's
%% pattern (for example `demo/general/#') to that rule's internal topic,
%% which no publisher uses.
-spec subscription_topic(binary(), list()) -> binary().
subscription_topic(Filter, AllowedTopics) ->
    case kraken_topics:resolve(Filter, AllowedTopics) of
        {exact, InternalTopic, _RoomId, AppId} ->
            case has_wildcard(Filter) of
                true -> kraken_topics:fallback_topic(AppId, Filter);
                false -> InternalTopic
            end;
        {wildcard, FallbackTopic, _AppId, _Rule} ->
            FallbackTopic;
        no_match ->
            kraken_topics:fallback_topic(<<"unscoped">>, Filter)
    end.

%% Topic to show an MQTT subscriber for a message published on broker topic
%% PublishedTopic, given the connection's subscriptions (filter => broker
%% topic). A subscription without wildcards shows its own filter; a wildcard
%% subscription (broker topic <app_id>/<filter>) shows the published topic
%% with the <app_id>/ prefix removed. undefined when nothing matches.
-spec display_topic(binary(), #{binary() => binary()}) -> binary() | undefined.
display_topic(PublishedTopic, Subscriptions) ->
    Matching = [{Filter, BrokerTopic} || {Filter, BrokerTopic} <- maps:to_list(Subscriptions),
                                         kraken_acl:matches_pattern(PublishedTopic, BrokerTopic)],
    case lists:partition(fun({_, BrokerTopic}) -> not has_wildcard(BrokerTopic) end, Matching) of
        {[{Filter, _} | _], _} ->
            Filter;
        {[], [{Filter, BrokerTopic} | _]} ->
            PrefixLen = byte_size(BrokerTopic) - byte_size(Filter),
            <<Prefix:PrefixLen/binary, _/binary>> = BrokerTopic,
            case PublishedTopic of
                <<Prefix:PrefixLen/binary, Display/binary>> when Display =/= <<>> -> Display;
                _ -> undefined
            end;
        {[], []} ->
            undefined
    end.

%% Bytes to deliver to an MQTT subscriber for a broker payload, or drop when
%% the message was published by this same connection (no-echo).
%%
%% Broker payloads are MessagePack: the data itself, optionally wrapped in a
%% delivery-tracking #{_msgId, _data} map, optionally inside a no-echo
%% #{data, _sender} envelope. The wrappers are removed; binary data is sent
%% as its raw bytes and any other data as its MessagePack encoding. A payload
%% that is not MessagePack at all (published to an external broker by
%% something other than kraken) is forwarded untouched.
-spec delivery_payload(binary(), binary() | undefined) -> {ok, binary()} | drop.
delivery_payload(Packed, ConnectionId) ->
    case catch msgpack:unpack(Packed, [{unpack_str, as_binary}]) of
        {ok, Decoded} ->
            case unwrap(Decoded) of
                {Sender, _Data} when is_binary(Sender), Sender =:= ConnectionId ->
                    drop;
                {_Sender, Data} when is_binary(Data) ->
                    {ok, Data};
                {_Sender, Data} ->
                    case kraken_msgpack:pack(Data) of
                        Bytes when is_binary(Bytes) -> {ok, Bytes};
                        {error, _} -> {ok, Packed}
                    end
            end;
        _ ->
            {ok, Packed}
    end.

unwrap(#{<<"_sender">> := Sender, <<"data">> := Inner}) ->
    {Sender, strip_msg_id(Inner)};
unwrap(Decoded) ->
    {undefined, strip_msg_id(Decoded)}.

strip_msg_id(#{<<"_msgId">> := _MsgId, <<"_data">> := Data}) -> Data;
strip_msg_id(Data) -> Data.

has_wildcard(Topic) ->
    binary:match(Topic, [<<"+">>, <<"#">>]) =/= nomatch.

%% Check and update rate limit counter. Publishes over the limit still count,
%% so the caller can tell the first one apart.
check_rate_limit(State) ->
    CurrentSecond = erlang:system_time(second),
    RateLimitSecond = State#state.rate_limit_second,
    MsgCount = State#state.msg_count,
    RateLimit = State#state.rate_limit,

    case CurrentSecond of
        RateLimitSecond ->
            case MsgCount >= RateLimit of
                true ->
                    {error, rate_limited, State#state{msg_count = MsgCount + 1}};
                false ->
                    {ok, State#state{msg_count = MsgCount + 1}}
            end;
        _ ->
            {ok, State#state{msg_count = 1, rate_limit_second = CurrentSecond}}
    end.

notify_usage(undefined, _Bytes) -> ok;
notify_usage(ProjectId, Bytes) ->
    catch kraken_usage:increment(ProjectId, 1, Bytes).

%% Record message to Firestore if enabled. Payload is the raw MQTT payload.
maybe_record_message(FirestoreWriter, Pattern, InternalTopic, Payload, Context, ActorTokenId) ->
    %% Always track usage regardless of whether message recording is enabled
    ProjectId = maps:get(project_id, Context, undefined),
    notify_usage(ProjectId, byte_size(Payload)),

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
            %% Pack only when recording, with the same encoding the broker
            %% uses, so stored bytes do not depend on the ingress protocol
            PackedPayload = kraken_msgpack:pack(Payload),
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
