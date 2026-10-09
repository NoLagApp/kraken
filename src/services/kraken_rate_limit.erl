%%%-------------------------------------------------------------------
%% @doc Per-key, per-second message budget for callers that have no
%% connection to hang a counter on (the HTTP publish route).
%%
%% A fixed one-second window in a shared ETS table. Approximate under
%% concurrent requests for the same key, which is fine for a rate limit.
%% The table is owned by kraken_sup.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_rate_limit).

-export([ensure_table/0, allow/3]).

-define(TABLE, kraken_rate_limit).

ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined ->
            try
                ets:new(?TABLE, [named_table, public, set, {write_concurrency, true}])
            catch
                error:badarg -> ok
            end,
            ok;
        _ ->
            ok
    end.

%% Spend N from Key's budget of Limit per second.
-spec allow(term(), pos_integer(), pos_integer()) -> ok | {error, rate_limited}.
allow(Key, N, Limit) ->
    ensure_table(),
    Sec = erlang:monotonic_time(second),
    try ets:lookup(?TABLE, Key) of
        [{Key, Sec, Count}] when Count + N > Limit ->
            {error, rate_limited};
        [{Key, Sec, _}] ->
            ets:update_counter(?TABLE, Key, {3, N}),
            ok;
        _ when N > Limit ->
            {error, rate_limited};
        _ ->
            ets:insert(?TABLE, {Key, Sec, N}),
            ok
    catch
        error:badarg -> ok
    end.
