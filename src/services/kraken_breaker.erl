%%%-------------------------------------------------------------------
%% @doc Minimal consecutive-failure circuit breaker.
%%
%% Guards inline calls to a remote dependency whose timeout would
%% otherwise block a hot path once the dependency is unhealthy. The
%% subscribe cache-miss fallback is the motivating caller: a per-check
%% 2s control-plane call, made in the connection process — fine in
%% isolation, but during a control-plane outage a reconnect storm turns
%% every distinct (actor, pattern) miss into a 2s stall.
%%
%% State machine, keyed by an arbitrary breaker Name (atom):
%%   closed    — calls allowed; consecutive failures counted.
%%   open      — after >= threshold consecutive failures; allow/1 returns
%%               false for a cooldown so callers skip the doomed call and
%%               fail closed cheaply.
%%   half_open — after the cooldown, allow/1 lets ONE probe through;
%%               record_success closes, record_failure re-opens.
%%
%% All state lives in one public ETS table (lazy-created, like
%% kraken_acl's deny cache). No process; callers drive transitions via
%% allow/record_success/record_failure. Lost races on first-use table
%% creation are swallowed.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_breaker).

-export([allow/1, record_success/1, record_failure/1]).

-define(TABLE, kraken_breaker_state).
-define(DEFAULT_FAILURE_THRESHOLD, 5).
-define(DEFAULT_COOLDOWN_MS, 10000).

%% Per-breaker record: {Name, State, ConsecutiveFailures, OpenedUntil}
%%   State :: closed | open | half_open
%%   OpenedUntil :: monotonic ms when the cooldown ends (0 when closed).

%% Should a call be attempted? Advances open -> half_open once the
%% cooldown has elapsed, admitting a single probe.
-spec allow(Name :: atom()) -> boolean().
allow(Name) ->
    ensure_table(),
    case ets:lookup(?TABLE, Name) of
        [] ->
            true;
        [{Name, closed, _F, _U}] ->
            true;
        [{Name, half_open, _F, _U}] ->
            %% A probe is already in flight; hold others closed until it settles.
            false;
        [{Name, open, F, OpenedUntil}] ->
            case now_ms() >= OpenedUntil of
                true ->
                    %% Cooldown elapsed: promote to half-open and admit this one probe.
                    ets:insert(?TABLE, {Name, half_open, F, OpenedUntil}),
                    true;
                false ->
                    false
            end
    end.

%% The guarded call succeeded (or the dependency answered healthily):
%% reset to closed.
-spec record_success(Name :: atom()) -> ok.
record_success(Name) ->
    ensure_table(),
    ets:insert(?TABLE, {Name, closed, 0, 0}),
    ok.

%% The guarded call failed in a way that indicates the dependency is
%% unhealthy (e.g. a timeout). Count it; trip to open at the threshold,
%% and re-open immediately on a failed half-open probe.
-spec record_failure(Name :: atom()) -> ok.
record_failure(Name) ->
    ensure_table(),
    case ets:lookup(?TABLE, Name) of
        [{Name, half_open, _F, _U}] ->
            open(Name);
        [{Name, _State, F, _U}] ->
            case F + 1 >= failure_threshold() of
                true -> open(Name);
                false -> ets:insert(?TABLE, {Name, closed, F + 1, 0})
            end;
        [] ->
            case 1 >= failure_threshold() of
                true -> open(Name);
                false -> ets:insert(?TABLE, {Name, closed, 1, 0})
            end
    end,
    ok.

%%====================================================================
%% Internal
%%====================================================================

open(Name) ->
    ets:insert(?TABLE, {Name, open, failure_threshold(), now_ms() + cooldown_ms()}),
    ok.

now_ms() ->
    erlang:monotonic_time(millisecond).

ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined ->
            try
                ets:new(?TABLE,
                    [named_table, public, set, {read_concurrency, true},
                     {write_concurrency, true}])
            catch
                error:badarg -> ok
            end,
            ok;
        _ ->
            ok
    end.

failure_threshold() ->
    case application:get_env(kraken, breaker_failure_threshold) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_FAILURE_THRESHOLD
    end.

cooldown_ms() ->
    case application:get_env(kraken, breaker_cooldown_ms) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_COOLDOWN_MS
    end.
