%%%-------------------------------------------------------------------
%% @doc MQTT Bridge (EMQX Implementation)
%% Connects to EMQX and bridges messages to/from WebSocket clients.
%% Supports shared subscriptions for load balancing (MQTT 5.0).
%%
%% This is the default message_bridge implementation.
%% To use a different broker, implement the message_bridge behaviour
%% and configure it in sys.config.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_broker_mqtt).
-behaviour(kraken_broker).

-export([session_options/4]).
-export([
    start/0,
    capabilities/0,
    connect/0,
    connect/1,
    connect/3,
    subscribe/3,
    subscribe/4,
    subscribe/5,
    unsubscribe/2,
    publish/3,
    publish/4,
    publish/5,
    publish/6,
    disconnect/1,
    format_shared_subscription/2,
    supports_load_balancing/0
]).

%% App-level init: nothing to prepare; connections are per-session.
start() ->
    ok.

capabilities() ->
    #{retained => true, shared_subscriptions => true, multi_region => true}.

%% Connect to EMQX without actor credentials (for MQTT handler)
%% Uses a unique ID per connection
connect() ->
    {ok, EmqxHost} = application:get_env(kraken, mqtt_broker_host),
    {ok, EmqxPort} = application:get_env(kraken, mqtt_broker_port),

    UniqueId = integer_to_binary(erlang:unique_integer([positive])),
    ClientId = <<"kraken_mqtt_", UniqueId/binary>>,

    WsPid = self(),

    Options = #{
        host => EmqxHost,
        port => EmqxPort,
        clientid => ClientId,
        clean_start => true,
        keepalive => 60,
        msg_handler => #{
            publish => fun(Msg) -> WsPid ! {mqtt_publish, Msg} end,
            disconnected => fun(_Reason) -> WsPid ! mqtt_disconnected end
        }
    },

    case emqtt:start_link(with_credentials(Options)) of
        {ok, Client} ->
            case emqtt:connect(Client) of
                {ok, _Props} ->
                    {ok, Client};
                {error, Reason} ->
                    kraken_log:error("[MQTT Bridge] Connect failed: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Connect to EMQX with actor credentials (non-persistent, backward compat)
connect(AuthData) ->
    connect(AuthData, false, 0).

%% How this connection identifies its MQTT session.
%%
%% Persistent sessions use a stable ClientId so the broker can resume across
%% reconnects. The key is the session key rather than the actor id, because a
%% session belongs to a client instance and not to a credential — see
%% kraken_session. Non-persistent sessions take a unique suffix, which is what
%% lets many concurrent connections share one token.
%%
%% The expiry has to travel as an MQTT 5 CONNECT property. emqtt speaks 3.1.1
%% by default and silently drops options it does not recognise, so the
%% `session_expiry_interval' passed here previously never reached the broker at
%% all: sessions were kept under 3.1.1 rules, which is to say for ever. Only
%% persistent connections switch to v5; a clean session has no expiry to
%% express and no reason to change protocol.
%%
%% An expiry of zero or less means the control plane did not configure one.
%% That is sent as the "never expire" value rather than as a literal 0, which
%% in v5 would end the session at disconnect and silently throw away the queue
%% this whole mechanism exists to keep.
-define(SESSION_NEVER_EXPIRES, 16#FFFFFFFF).

-spec session_options(boolean(), binary(), binary(), integer()) ->
    {binary(), boolean(), map()}.
session_options(true, _ActorTokenId, SessionKey, SessionExpirySeconds) ->
    Expiry = case SessionExpirySeconds of
        N when is_integer(N), N > 0 -> N;
        _ -> ?SESSION_NEVER_EXPIRES
    end,
    {<<"kraken_agent_", SessionKey/binary>>, false,
     #{proto_ver => v5,
       properties => #{'Session-Expiry-Interval' => Expiry}}};
session_options(false, ActorTokenId, _SessionKey, _SessionExpirySeconds) ->
    UniqueId = integer_to_binary(erlang:unique_integer([positive])),
    {<<"kraken_proxy_", ActorTokenId/binary, "_", UniqueId/binary>>, true, #{}}.

%% Connect to EMQX with actor credentials and persistent session config
%% PersistentSession: true for agents/orchestrators, false for others
%% SessionExpirySeconds: MQTT 5.0 session expiry (only used when PersistentSession = true)
connect(AuthData, PersistentSession, SessionExpirySeconds) ->
    {ok, EmqxHost} = application:get_env(kraken, mqtt_broker_host),
    {ok, EmqxPort} = application:get_env(kraken, mqtt_broker_port),

    ActorTokenId = maps:get(actor_token_id, AuthData),

    SessionKey = maps:get(session_key, AuthData, ActorTokenId),
    {ClientId, CleanStart, ExpiryProps} =
        session_options(PersistentSession, ActorTokenId, SessionKey, SessionExpirySeconds),

    %% Get the calling process (WebSocket handler) to forward messages to it
    WsPid = self(),

    Options = #{
        host => EmqxHost,
        port => EmqxPort,
        clientid => ClientId,
        clean_start => CleanStart,
        keepalive => 60,
        %% Message handler - forward to WebSocket process
        msg_handler => #{
            publish => fun(Msg) -> WsPid ! {mqtt_publish, Msg} end,
            disconnected => fun(_Reason) -> WsPid ! mqtt_disconnected end
        }
    },

    FinalOptions = maps:merge(Options, ExpiryProps),

    case emqtt:start_link(with_credentials(FinalOptions)) of
        {ok, Client} ->
            case emqtt:connect(Client) of
                {ok, _Props} ->
                    {ok, Client, ClientId};
                {error, Reason} ->
                    kraken_log:error("[MQTT Bridge] Connect failed: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Subscribe to a topic (simple - MQTT topic = display topic)
subscribe(Client, Topic, WsPid) ->
    subscribe(Client, Topic, Topic, WsPid, 1).

%% Subscribe to a topic (with topic mapping, default QoS 1)
subscribe(Client, MqttTopic, DisplayTopic, WsPid) ->
    subscribe(Client, MqttTopic, DisplayTopic, WsPid, 1).

%% Subscribe to a topic (with topic mapping and QoS)
%% MqttTopic: The actual MQTT topic (may include $share/group/ prefix)
%% DisplayTopic: The topic name to show to the client (without $share prefix)
subscribe(Client, MqttTopic, DisplayTopic, WsPid, QoS) ->
    %% Store the WebSocket PID and display topic for message routing in WsPid's process dict
    %% We send a message to WsPid to store the mapping
    WsPid ! {store_topic_mapping, MqttTopic, DisplayTopic},

    %% Subscribe with requested QoS
    case emqtt:subscribe(Client, MqttTopic, QoS) of
        {ok, _Props, _ReasonCodes} ->
            ok;
        {error, Reason} ->
            kraken_log:error("[MQTT] Subscribe FAILED for ~s: ~p", [MqttTopic, Reason])
    end,
    ok.

%% Unsubscribe from a topic
unsubscribe(Client, Topic) ->
    %% Try to find the MQTT topic (could be shared subscription)
    %% For now, assume Topic is the display topic
    erase({topic_owner, Topic}),
    {ok, _, _} = emqtt:unsubscribe(Client, Topic),
    ok.

%% Publish a message (echo enabled - no sender info, default QoS 1)
publish(Client, Topic, Data) ->
    publish_internal(Client, Topic, Data, undefined, 1, false).

%% Publish a message with sender info (echo disabled, default QoS 1)
publish(Client, Topic, Data, Sender) ->
    publish_internal(Client, Topic, Data, Sender, 1, false).

%% Publish a message with sender info and QoS
publish(Client, Topic, Data, Sender, QoS) ->
    publish_internal(Client, Topic, Data, Sender, QoS, false).

%% Publish a message with sender info, QoS, and retain flag
publish(Client, Topic, Data, Sender, QoS, Retain) ->
    publish_internal(Client, Topic, Data, Sender, QoS, Retain).

publish_internal(Client, Topic, Data, undefined, QoS, Retain) ->
    %% Encode data as MessagePack (with binary as string for JS compatibility;
    %% a binary that is not valid UTF-8 encodes as bin, see kraken_msgpack)
    Payload = kraken_msgpack:pack(Data),
    %% emqtt:publish/4 accepts [pubopt()] as 4th arg: [{qos, N}, {retain, Bool}]
    Opts = [{qos, QoS}] ++ case Retain of true -> [{retain, true}]; _ -> [] end,
    emqtt:publish(Client, Topic, Payload, Opts),
    ok;
publish_internal(Client, Topic, Data, Sender, QoS, Retain) ->
    %% Wraps data in envelope: #{data => Data, _sender => Sender}
    Envelope = #{<<"data">> => Data, <<"_sender">> => Sender},
    Payload = kraken_msgpack:pack(Envelope),
    Opts = [{qos, QoS}] ++ case Retain of true -> [{retain, true}]; _ -> [] end,
    emqtt:publish(Client, Topic, Payload, Opts),
    ok.

%% Disconnect from EMQX
disconnect(Client) ->
    emqtt:disconnect(Client),
    ok.

%% Format a shared subscription topic (MQTT 5.0 format)
%% EMQX uses: $share/group_name/actual/topic
format_shared_subscription(BaseTopic, Group) ->
    <<"$share/", Group/binary, "/", BaseTopic/binary>>.

%% EMQX supports shared subscriptions
supports_load_balancing() ->
    true.

%%====================================================================
%% Internal functions
%%====================================================================

%% emqtt requires username/password keys to be ABSENT when not used
%% (it crashes on undefined values), so add them conditionally.
with_credentials(Opts) ->
    O1 = case broker_username() of
        undefined -> Opts;
        U -> Opts#{username => U}
    end,
    case broker_password() of
        undefined -> O1;
        P -> O1#{password => P}
    end.

broker_username() ->
    case application:get_env(kraken, mqtt_broker_username) of
        {ok, U} when is_list(U), U =/= [] -> list_to_binary(U);
        {ok, U} when is_binary(U), U =/= <<>> -> U;
        _ -> undefined
    end.

broker_password() ->
    case application:get_env(kraken, mqtt_broker_password) of
        {ok, P} when is_list(P), P =/= [] -> list_to_binary(P);
        {ok, P} when is_binary(P), P =/= <<>> -> P;
        _ -> undefined
    end.
