%%%-------------------------------------------------------------------
%% @doc Persistent Presence integration test — drives the real behaviour
%% dispatch (kraken_presence_store / kraken_wake) through the full loop:
%% advertise(online) -> discover -> soft-offline -> discover -> publish-gate
%% (mark_waking + wake fire). Uses ETS/in-memory test backends (no Firestore).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_persistent_presence_tests).
-include_lib("eunit/include/eunit.hrl").

setup_backends() ->
    application:set_env(kraken, presence_store_backend, pp_test_store),
    application:set_env(kraken, wake_backend, pp_test_wake),
    pp_test_store:reset(),
    pp_test_wake:reset(),
    ok.

%% Slots resolve to the configured custom backends.
backend_resolution_test() ->
    setup_backends(),
    ?assertEqual(pp_test_store, kraken:backend(presence_store)),
    ?assertEqual(pp_test_wake, kraken:backend(wake)).

%% Defaults are no-op: OSS/syn builds keep ephemeral presence (discover -> []).
noop_defaults_test() ->
    application:unset_env(kraken, presence_store_backend),
    application:unset_env(kraken, wake_backend),
    ?assertEqual(kraken_presence_store_noop, kraken:backend(presence_store)),
    ?assertEqual(kraken_wake_noop, kraken:backend(wake)),
    ?assertEqual({ok, []}, kraken_presence_store:discover(#{room_id => <<"r">>})),
    ?assertEqual(ok, kraken_wake:fire(#{<<"wakeUrl">> => <<"http://x">>})).

%% Full advertise -> offline -> discover -> wake loop.
full_loop_test() ->
    setup_backends(),
    R = #{app_id => <<"app1">>, room_id => <<"room1">>, actor_token_id => <<"echo">>,
          advertisement => #{<<"capabilities">> => [<<"soil_analysis">>]},
          wake => #{<<"url">> => <<"http://localhost:9999/wake">>}},

    %% advertise persistent -> online + discoverable
    ok = kraken_presence_store:upsert(R),
    {ok, [A1]} = kraken_presence_store:discover(#{room_id => <<"room1">>, status => <<"online">>}),
    ?assertEqual(<<"echo">>, maps:get(<<"actorTokenId">>, A1)),

    %% disconnect -> soft-offline (still discoverable, now offline)
    ok = kraken_presence_store:offline(R),
    ?assertEqual({ok, []}, kraken_presence_store:discover(#{room_id => <<"room1">>, status => <<"online">>})),
    {ok, [A2]} = kraken_presence_store:discover(#{room_id => <<"room1">>, status => <<"offline">>}),
    ?assertEqual(<<"offline">>, maps:get(<<"status">>, A2)),

    %% publish-gate: discover offline -> mark_waking + fire wake (mirrors pp_wake_offline)
    {ok, Offline} = kraken_presence_store:discover(#{room_id => <<"room1">>, status => <<"offline">>}),
    lists:foreach(fun(Act) ->
        ok = kraken_presence_store:mark_waking(Act),
        ok = kraken_wake:fire(Act)
    end, Offline),

    %% wake fired exactly once, carrying the registered wake URL
    Fires = pp_test_wake:fires(),
    ?assertEqual(1, length(Fires)),
    [Fired] = Fires,
    ?assertEqual(<<"http://localhost:9999/wake">>, maps:get(<<"wakeUrl">>, Fired)),

    %% status is now waking (debounces further wakes)
    {ok, [A3]} = kraken_presence_store:discover(#{room_id => <<"room1">>, status => <<"waking">>}),
    ?assertEqual(<<"echo">>, maps:get(<<"actorTokenId">>, A3)),
    ok.
