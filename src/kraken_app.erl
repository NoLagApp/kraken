%%%-------------------------------------------------------------------
%% @doc Kraken Application
%% @end
%%%-------------------------------------------------------------------
-module(kraken_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    %% SYN scopes: presence, lobbies, per-org connection tracking, and
    %% topic groups for the built-in syn broker backend.
    syn:start(),
    syn:add_node_to_scopes([kraken_presence, kraken_lobbies, kraken_connections, kraken_topics]),

    %% httpc connection pooling (used by misc outbound HTTP)
    httpc:set_options([
        {max_sessions, 64},
        {max_keep_alive_length, 16},
        {keep_alive_timeout, 120000},
        {max_pipeline_length, 0},
        {pipeline_timeout, 0}
    ], default),

    {ok, WsPort} = application:get_env(kraken, ws_port),
    MqttPort = application:get_env(kraken, mqtt_port, 1883),

    %% Broker backend app-level init (retained tables, pools, ...)
    Broker = kraken:backend(broker),
    ok = Broker:start(),

    Dispatch = cowboy_router:compile([
        {'_', [
            {"/ws", kraken_ws_handler, []},
            {"/health", kraken_health_handler, []},
            {"/internal/publish", kraken_internal_handler, []}
        ]}
    ]),

    {ok, _} = cowboy:start_clear(
        kraken_ws_listener,
        #{
            socket_opts => [
                {port, WsPort},
                {nodelay, true},
                {sndbuf, 65536},
                {recbuf, 65536},
                {backlog, 2048}
            ],
            num_acceptors => 100
        },
        #{env => #{dispatch => Dispatch}}
    ),
    io:format("Kraken WebSocket started on port ~p~n", [WsPort]),

    {ok, _} = ranch:start_listener(
        kraken_mqtt_listener,
        ranch_tcp,
        #{socket_opts => [{port, MqttPort}], num_acceptors => 10},
        kraken_mqtt_handler,
        []
    ),
    io:format("Kraken MQTT ingress started on port ~p~n", [MqttPort]),

    kraken_sup:start_link().

stop(_State) ->
    ok = cowboy:stop_listener(kraken_ws_listener),
    ok = ranch:stop_listener(kraken_mqtt_listener),
    ok.
