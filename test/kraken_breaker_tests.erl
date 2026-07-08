-module(kraken_breaker_tests).
-include_lib("eunit/include/eunit.hrl").

%% Consecutive-failure circuit breaker used by the WS handler's subscribe
%% cache-miss fallback to stop hammering (and blocking on) an unhealthy
%% control plane. Each test uses a unique breaker name so the shared ETS
%% table doesn't couple them.

setup() ->
    application:set_env(kraken, breaker_failure_threshold, 3),
    application:set_env(kraken, breaker_cooldown_ms, 50),
    ok.

cleanup(_) ->
    application:unset_env(kraken, breaker_failure_threshold),
    application:unset_env(kraken, breaker_cooldown_ms),
    ok.

breaker_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     [
      fun unknown_breaker_allows/0,
      fun trips_after_threshold_failures/0,
      fun success_resets_failure_count/0,
      fun half_open_probe_then_close/0,
      fun half_open_probe_failure_reopens/0
     ]}.

unknown_breaker_allows() ->
    ?assert(kraken_breaker:allow(unknown_b1)).

trips_after_threshold_failures() ->
    N = trips_after_threshold_failures,
    ?assert(kraken_breaker:allow(N)),
    kraken_breaker:record_failure(N),   %% 1
    ?assert(kraken_breaker:allow(N)),
    kraken_breaker:record_failure(N),   %% 2
    ?assert(kraken_breaker:allow(N)),
    kraken_breaker:record_failure(N),   %% 3 == threshold -> open
    ?assertNot(kraken_breaker:allow(N)).

success_resets_failure_count() ->
    N = success_resets_failure_count,
    kraken_breaker:record_failure(N),
    kraken_breaker:record_failure(N),
    kraken_breaker:record_success(N),   %% reset to closed/0
    kraken_breaker:record_failure(N),
    kraken_breaker:record_failure(N),
    %% Only 2 consecutive since reset (< threshold 3) -> still closed.
    ?assert(kraken_breaker:allow(N)).

half_open_probe_then_close() ->
    N = half_open_probe_then_close,
    trip(N),
    ?assertNot(kraken_breaker:allow(N)),
    timer:sleep(70),                    %% > cooldown 50ms
    ?assert(kraken_breaker:allow(N)),   %% half-open: one probe admitted
    ?assertNot(kraken_breaker:allow(N)),%% concurrent callers held closed
    kraken_breaker:record_success(N),   %% probe ok -> closed
    ?assert(kraken_breaker:allow(N)).

half_open_probe_failure_reopens() ->
    N = half_open_probe_failure_reopens,
    trip(N),
    timer:sleep(70),
    ?assert(kraken_breaker:allow(N)),   %% probe admitted
    kraken_breaker:record_failure(N),   %% probe fails -> re-open immediately
    ?assertNot(kraken_breaker:allow(N)).

trip(N) ->
    kraken_breaker:record_failure(N),
    kraken_breaker:record_failure(N),
    kraken_breaker:record_failure(N).
