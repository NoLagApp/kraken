%%%-------------------------------------------------------------------
%% @doc kraken_resume tests: what a reconnecting connection gets restored.
%%
%% Uses a real syn scope, because retention depends on whether another
%% connection still holds the key, which is membership behaviour.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_resume_tests).
-include_lib("eunit/include/eunit.hrl").

-define(KEY, {<<"actor-1">>, undefined}).

setup() ->
    application:ensure_all_started(syn),
    catch syn:add_node_to_scopes([kraken_resume]),
    case whereis(kraken_resume) of
        undefined -> {ok, _} = kraken_resume:start_link(), ok;
        _ -> ok
    end,
    application:unset_env(kraken, resume_retention_ms),
    kraken_resume:clear(?KEY),
    kraken_resume:clear({<<"actor-1">>, <<"tab-a">>}),
    kraken_resume:clear({<<"actor-1">>, <<"tab-b">>}),
    catch syn:leave(kraken_resume, {resume, ?KEY}, self()),
    ok.

topics(Requests) ->
    lists:sort([maps:get(<<"topic">>, R) || R <- Requests]).

%% A peer "connection" that holds Key until told to release it.
with_peer(Key, Fun) ->
    Owner = self(),
    Peer = spawn(fun() ->
        kraken_resume:register_connection(Key),
        Owner ! {registered, self()},
        receive release -> ok after 5000 -> ok end
    end),
    receive {registered, Peer} -> ok after 5000 -> erlang:error(peer_never_registered) end,
    try Fun() after Peer ! release end.

%%====================================================================

remembers_only_replayable_fields_test() ->
    setup(),
    ok = kraken_resume:remember(?KEY, <<"demo/general/messages">>,
        #{<<"type">> => <<"subscribe">>, <<"topic">> => <<"demo/general/messages">>,
          <<"filters">> => [<<"x">>], <<"qos">> => 2, <<"msgRef">> => <<"m1">>}),
    ?assertEqual([#{<<"topic">> => <<"demo/general/messages">>,
                    <<"filters">> => [<<"x">>], <<"qos">> => 2}],
                 kraken_resume:recall(?KEY)).

set_filters_and_unsubscribe_are_reflected_test() ->
    setup(),
    kraken_resume:remember(?KEY, <<"a/r/t1">>, #{<<"filters">> => [<<"x">>]}),
    kraken_resume:remember(?KEY, <<"a/r/t2">>, #{}),
    kraken_resume:update_filters(?KEY, <<"a/r/t1">>, [<<"y">>, <<"z">>]),
    [T1] = [R || R <- kraken_resume:recall(?KEY), maps:get(<<"topic">>, R) =:= <<"a/r/t1">>],
    ?assertEqual([<<"y">>, <<"z">>], maps:get(<<"filters">>, T1)),
    kraken_resume:update_filters(?KEY, <<"a/r/t1">>, []),
    [T1b] = [R || R <- kraken_resume:recall(?KEY), maps:get(<<"topic">>, R) =:= <<"a/r/t1">>],
    ?assertNot(maps:is_key(<<"filters">>, T1b)),
    kraken_resume:forget(?KEY, <<"a/r/t2">>),
    ?assertEqual([<<"a/r/t1">>], topics(kraken_resume:recall(?KEY))).

take_returns_and_clears_test() ->
    setup(),
    kraken_resume:remember(?KEY, <<"a/r/t1">>, #{}),
    kraken_resume:remember(?KEY, <<"a/r/t2">>, #{}),
    ?assertEqual([<<"a/r/t1">>, <<"a/r/t2">>], topics(kraken_resume:take(?KEY))),
    ?assertEqual([], kraken_resume:recall(?KEY)).

client_ids_keep_sets_apart_test() ->
    setup(),
    KeyA = kraken_resume:key(<<"actor-1">>, <<"tab-a">>),
    KeyB = kraken_resume:key(<<"actor-1">>, <<"tab-b">>),
    kraken_resume:remember(KeyA, <<"a/r/only-a">>, #{}),
    kraken_resume:remember(KeyB, <<"a/r/only-b">>, #{}),
    ?assertEqual([<<"a/r/only-a">>], topics(kraken_resume:recall(KeyA))),
    ?assertEqual([<<"a/r/only-b">>], topics(kraken_resume:recall(KeyB))),
    %% No clientId: one shared key per actor
    ?assertEqual(?KEY, kraken_resume:key(<<"actor-1">>, undefined)).

fresh_connection_drops_leftovers_test() ->
    setup(),
    kraken_resume:remember(?KEY, <<"a/r/stale">>, #{}),
    kraken_resume:fresh_connection(?KEY),
    ?assertEqual([], kraken_resume:recall(?KEY)).

fresh_connection_keeps_a_live_peers_set_test() ->
    setup(),
    kraken_resume:remember(?KEY, <<"a/r/peer">>, #{}),
    with_peer(?KEY, fun() ->
        kraken_resume:fresh_connection(?KEY),
        ?assertEqual([<<"a/r/peer">>], topics(kraken_resume:recall(?KEY)))
    end).

expires_after_retention_once_nobody_holds_the_key_test() ->
    setup(),
    application:set_env(kraken, resume_retention_ms, 0),
    kraken_resume:register_connection(?KEY),
    kraken_resume:remember(?KEY, <<"a/r/t1">>, #{}),
    kraken_resume:connection_closed(?KEY),
    timer:sleep(5),
    sweep(),
    ?assertEqual([], kraken_resume:recall(?KEY)),
    application:unset_env(kraken, resume_retention_ms).

kept_while_another_connection_holds_the_key_test() ->
    setup(),
    application:set_env(kraken, resume_retention_ms, 0),
    kraken_resume:remember(?KEY, <<"a/r/t1">>, #{}),
    with_peer(?KEY, fun() ->
        kraken_resume:register_connection(?KEY),
        kraken_resume:connection_closed(?KEY),
        timer:sleep(5),
        sweep(),
        ?assertEqual([<<"a/r/t1">>], topics(kraken_resume:recall(?KEY)))
    end),
    application:unset_env(kraken, resume_retention_ms).

%% Run one sweep and wait for it to finish (the call is ordered after it).
sweep() ->
    kraken_resume ! sweep,
    ok = gen_server:call(kraken_resume, sync).
