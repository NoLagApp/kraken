%%%-------------------------------------------------------------------
%% @doc Test kraken_wake backend that records each fire so the
%% persistent-presence integration test can assert wakes happened.
%% @end
%%%-------------------------------------------------------------------
-module(pp_test_wake).
-behaviour(kraken_wake).

-export([fire/1, fires/0, reset/0]).

ensure_collector() ->
    case whereis(pp_test_wake_collector) of
        undefined ->
            Pid = spawn(fun() -> collector([]) end),
            register(pp_test_wake_collector, Pid);
        _ ->
            ok
    end.

collector(Acc) ->
    receive
        {fire, A} -> collector([A | Acc]);
        {get, From} -> From ! {fires, lists:reverse(Acc)}, collector(Acc);
        reset -> collector([])
    end.

reset() ->
    ensure_collector(),
    pp_test_wake_collector ! reset,
    ok.

fire(Actor) ->
    ensure_collector(),
    pp_test_wake_collector ! {fire, Actor},
    ok.

fires() ->
    ensure_collector(),
    pp_test_wake_collector ! {get, self()},
    receive {fires, L} -> L after 1000 -> [] end.
