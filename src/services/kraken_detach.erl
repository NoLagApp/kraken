%%%-------------------------------------------------------------------
%% @doc Detached execution for backend calls on hot paths.
%%
%% Connection processes (WS/MQTT frame loops) must never block on a
%% pluggable backend: with the built-in ets/noop backends every call is
%% microseconds, but a plugin may do remote I/O with multi-second
%% timeouts, and one inline call serializes every frame on that latency
%% (a per-publish presence lookup once throttled token streams to one
%% message per store-timeout — see kraken_presence_store).
%%
%% run/2 is the structural guard: it detaches before the work starts, so
%% the caller pays only a spawn. Use it in handlers for ANY backend call
%% whose result the frame path does not need. If the result IS needed,
%% the call does not belong on the frame path — cache it, precompute it,
%% or move the need out of band.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_detach).

-export([run/2]).

%% Run Fun in a detached process. Never blocks, never raises in the
%% caller. Failures are logged with Label instead of dying silently
%% (spawned funs produce no crash report a caller would ever correlate).
-spec run(Label :: atom() | binary() | string(), Fun :: fun(() -> any())) -> ok.
run(Label, Fun) when is_function(Fun, 0) ->
    spawn(fun() ->
        try
            Fun()
        catch
            Class:Reason:Stack ->
                kraken_log:error("[Detach] ~p failed: ~p:~p~n~p~n",
                                 [Label, Class, Reason, Stack])
        end
    end),
    ok.
