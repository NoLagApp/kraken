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
%% All state lives in one public ETS table owned by this supervised
%% gen_server, so a transient caller (a connection process handling a
%% subscribe) can't create it and then take it down on disconnect — that
%% would reset every breaker and badarg concurrent callers exactly during
%% the churn a breaker is meant to ride out. Callers drive transitions via
%% the module functions doing direct ETS ops (no gen_server round trip on
%% the hot path). ensure_table/0 is a lazy fallback for pre-boot / test use.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_breaker).
-behaviour(gen_server).

-export([start_link/0, allow/1, record_success/1, record_failure/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(TABLE, kraken_breaker_state).
-define(DEFAULT_FAILURE_THRESHOLD, 5).
-define(DEFAULT_COOLDOWN_MS, 10000).

%% Per-breaker record: {Name, State, ConsecutiveFailures, OpenedUntil}
%%   State :: closed | open | half_open
%%   OpenedUntil :: monotonic ms when the cooldown ends (0 when closed).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
%%
%% The counter bump uses ets:update_counter so concurrent failures — the
%% exact load the breaker exists for (a reconnect storm against a dead
%% dependency) — can't lose increments via a read-modify-write race and
%% leave the breaker closed past its threshold. The open() flip is not
%% atomic with the increment, but it is idempotent (concurrent trips just
%% re-stamp the same open state), so that race is harmless.
-spec record_failure(Name :: atom()) -> ok.
record_failure(Name) ->
    ensure_table(),
    %% Guarantee a row exists so update_counter can't badarg. Atomic; a no-op
    %% if the breaker is already tracked (won't clobber an open/half_open row).
    ets:insert_new(?TABLE, {Name, closed, 0, 0}),
    case ets:lookup(?TABLE, Name) of
        [{Name, open, _F, _U}] ->
            %% Already open — nothing to count.
            ok;
        [{Name, half_open, _F, _U}] ->
            %% Probe failed — straight back to open.
            open(Name);
        _ ->
            %% Closed: atomically increment the consecutive-failure count
            %% (position 3 of {Name, State, F, OpenedUntil}) and trip at the
            %% threshold.
            NewF = ets:update_counter(?TABLE, Name, {3, 1}),
            case NewF >= failure_threshold() of
                true -> open(Name);
                false -> ok
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

%%====================================================================
%% gen_server — owns the ETS table; no hot-path calls route through it.
%%====================================================================

init([]) ->
    ensure_table(),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
