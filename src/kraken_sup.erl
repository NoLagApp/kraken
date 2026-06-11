%%%-------------------------------------------------------------------
%% @doc Kraken top-level supervisor.
%%
%% Children are assembled dynamically: HTTP connection pools are only
%% started for backends that need them (auth=http / control=http), so
%% the zero-config deployment starts no outbound connections at all.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    Base = [
        worker(kraken_log, []),
        worker(kraken_cluster, []),
        worker(kraken_lobby_map, []),
        worker(kraken_usage, []),
        worker(kraken_subscriptions, [])
    ],

    AuthPool = http_pool(auth, auth_http_url, kraken_auth_pool),
    ControlPool = http_pool(control, control_http_url, kraken_control_pool),

    {ok, {SupFlags, Base ++ AuthPool ++ ControlPool}}.

worker(Module, Args) ->
    #{
        id => Module,
        start => {Module, start_link, Args},
        restart => permanent,
        type => worker,
        modules => [Module]
    }.

%% Start a small gun pool only when the slot uses the http backend.
http_pool(Slot, UrlKey, PoolName) ->
    case kraken:backend(Slot) of
        Module when Module =:= kraken_auth_http; Module =:= kraken_control_http ->
            case application:get_env(kraken, UrlKey) of
                {ok, Url} when is_list(Url), Url =/= "" ->
                    [#{
                        id => {gun_pool, PoolName, N},
                        start => {kraken_gun_client, start_link, [PoolName, Url, N]},
                        restart => permanent,
                        type => worker,
                        modules => [kraken_gun_client]
                    } || N <- [1, 2]];
                _ ->
                    error({missing_config, UrlKey, Slot})
            end;
        _ ->
            []
    end.
