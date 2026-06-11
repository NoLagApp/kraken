%%%-------------------------------------------------------------------
%% @doc Kraken public API.
%%
%% Backend resolution: each of the four plugin slots (auth, broker,
%% store, control) is configured via app env. Short names map to the
%% built-in modules; any other atom is treated as a custom module
%% implementing the corresponding behaviour (see docs/PLUGINS.md).
%%
%% stats/0 is the public introspection surface intended for embedding
%% applications (e.g. proprietary sidecars reporting node health to a
%% control plane).
%% @end
%%%-------------------------------------------------------------------
-module(kraken).

-export([backend/1, stats/0]).

%% Resolve the configured backend module for a plugin slot.
-spec backend(auth | broker | store | control) -> module().
backend(Slot) ->
    Default = default_for(Slot),
    Value = application:get_env(kraken, env_key(Slot), Default),
    resolve(Slot, Value).

env_key(auth) -> auth_backend;
env_key(broker) -> broker_backend;
env_key(store) -> store_backend;
env_key(control) -> control_backend.

default_for(auth) -> static;
default_for(broker) -> syn;
default_for(store) -> ets;
default_for(control) -> noop.

%% Short-name mapping; unknown atoms pass through as custom modules.
resolve(auth, static) -> kraken_auth_static;
resolve(auth, http) -> kraken_auth_http;
resolve(broker, syn) -> kraken_broker_syn;
resolve(broker, mqtt) -> kraken_broker_mqtt;
resolve(store, ets) -> kraken_store_ets;
resolve(store, noop) -> kraken_store_noop;
resolve(control, noop) -> kraken_control_noop;
resolve(control, http) -> kraken_control_http;
resolve(_Slot, Module) when is_atom(Module) -> Module.

%% Node introspection for embedding applications.
-spec stats() -> map().
stats() ->
    Broker = backend(broker),
    Capabilities =
        try Broker:capabilities()
        catch _:_ -> #{}
        end,
    OrgGroups =
        try syn:group_names(kraken_connections)
        catch _:_ -> []
        end,
    Connections = lists:foldl(
        fun(Group, Acc) ->
            Acc + length(syn:members(kraken_connections, Group))
        end, 0, OrgGroups),
    #{
        node => node(),
        cluster_nodes => [node() | nodes()],
        connections => Connections,
        organizations => length(OrgGroups),
        broker => Broker,
        broker_capabilities => Capabilities,
        store => backend(store),
        auth => backend(auth),
        control => backend(control)
    }.
