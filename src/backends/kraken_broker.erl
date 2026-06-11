%%%-------------------------------------------------------------------
%% @doc Broker behaviour + dispatcher.
%%
%% A broker backend provides fan-out between kraken connections. The
%% built-in backends are kraken_broker_syn (default, zero-dependency,
%% single cluster) and kraken_broker_mqtt (any MQTT 3.1.1 broker:
%% EMQX, Mosquitto, VerneMQ, ...).
%%
%% The dispatcher mirrors the historical mqtt_bridge API so connection
%% handlers stay backend-agnostic. Subscribed processes receive:
%%   {mqtt_publish, #{topic := Topic, payload := PackedPayload}}
%%   {store_topic_mapping, MqttTopic, DisplayTopic}
%% @end
%%%-------------------------------------------------------------------
-module(kraken_broker).

%% Behaviour
-callback start() -> ok.
-callback connect() -> {ok, Session :: term()} | {error, term()}.
-callback connect(AuthData :: map()) -> {ok, Session :: term()} | {error, term()}.
-callback connect(AuthData :: map(), PersistentSession :: boolean(), SessionExpirySeconds :: non_neg_integer()) ->
    {ok, Session :: term(), ClientId :: binary()} | {error, term()}.
-callback subscribe(Session :: term(), MqttTopic :: binary(), DisplayTopic :: binary(), WsPid :: pid(), QoS :: 0..2) -> ok.
-callback unsubscribe(Session :: term(), Topic :: binary()) -> ok.
-callback publish(Session :: term(), Topic :: binary(), Data :: term(), Sender :: binary() | undefined,
                  QoS :: 0..2, Retain :: boolean()) -> ok.
-callback disconnect(Session :: term()) -> ok.
-callback format_shared_subscription(BaseTopic :: binary(), Group :: binary()) -> binary().
-callback supports_load_balancing() -> boolean().
-callback capabilities() -> map().

%% Dispatcher API (mirrors mqtt_bridge)
-export([
    start/0,
    connect/0, connect/1, connect/3,
    subscribe/3, subscribe/4, subscribe/5,
    unsubscribe/2,
    publish/3, publish/4, publish/5, publish/6,
    disconnect/1,
    format_shared_subscription/2,
    supports_load_balancing/0,
    capabilities/0
]).

backend() -> kraken:backend(broker).

start() -> (backend()):start().

connect() -> (backend()):connect().
connect(AuthData) -> (backend()):connect(AuthData).
connect(AuthData, Persistent, Expiry) -> (backend()):connect(AuthData, Persistent, Expiry).

subscribe(Session, Topic, WsPid) ->
    subscribe(Session, Topic, Topic, WsPid, 1).
subscribe(Session, MqttTopic, DisplayTopic, WsPid) ->
    subscribe(Session, MqttTopic, DisplayTopic, WsPid, 1).
subscribe(Session, MqttTopic, DisplayTopic, WsPid, QoS) ->
    (backend()):subscribe(Session, MqttTopic, DisplayTopic, WsPid, QoS).

unsubscribe(Session, Topic) -> (backend()):unsubscribe(Session, Topic).

publish(Session, Topic, Data) ->
    publish(Session, Topic, Data, undefined, 1, false).
publish(Session, Topic, Data, Sender) ->
    publish(Session, Topic, Data, Sender, 1, false).
publish(Session, Topic, Data, Sender, QoS) ->
    publish(Session, Topic, Data, Sender, QoS, false).
publish(Session, Topic, Data, Sender, QoS, Retain) ->
    (backend()):publish(Session, Topic, Data, Sender, QoS, Retain).

disconnect(Session) -> (backend()):disconnect(Session).

format_shared_subscription(BaseTopic, Group) ->
    (backend()):format_shared_subscription(BaseTopic, Group).

supports_load_balancing() -> (backend()):supports_load_balancing().

capabilities() -> (backend()):capabilities().
