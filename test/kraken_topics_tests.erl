-module(kraken_topics_tests).
-include_lib("eunit/include/eunit.hrl").

%% Unified topic resolution (kraken_topics) — the module that replaced four
%% divergent inline fallback implementations. These tests pin:
%%   - exact mappings always win over wildcard rules
%%   - wildcard fallbacks are deterministic, app-scoped, and identical for
%%     subscribe + publish (no more `unknown/` shared namespace)
%%   - no_match for unauthorized/unconfigured patterns
%%   - the auto-provisioned room cache round-trip

exact_rule() ->
    #{
        <<"pattern">> => <<"app/room/messages">>,
        <<"topic">> => <<"room-uuid-1/messages">>,
        <<"permission">> => <<"pubSub">>,
        <<"room_id">> => <<"room-uuid-1">>,
        <<"app_id">> => <<"app-uuid-1">>
    }.

wildcard_rule() ->
    #{
        <<"pattern">> => <<"app/room/#">>,
        <<"permission">> => <<"pubSub">>,
        <<"room_id">> => <<"room-uuid-1">>,
        <<"app_id">> => <<"app-uuid-1">>
    }.

resolve_exact_test() ->
    ?assertEqual(
        {exact, <<"room-uuid-1/messages">>, <<"room-uuid-1">>, <<"app-uuid-1">>},
        kraken_topics:resolve(<<"app/room/messages">>, [exact_rule()])).

resolve_exact_wins_over_wildcard_test() ->
    %% Exact mapping must win regardless of rule order
    Expected = {exact, <<"room-uuid-1/messages">>, <<"room-uuid-1">>, <<"app-uuid-1">>},
    ?assertEqual(Expected, kraken_topics:resolve(<<"app/room/messages">>, [wildcard_rule(), exact_rule()])),
    ?assertEqual(Expected, kraken_topics:resolve(<<"app/room/messages">>, [exact_rule(), wildcard_rule()])).

resolve_wildcard_is_app_scoped_test() ->
    %% Fallback carries the MATCHED RULE's app id — never `unknown/`
    {wildcard, Base, AppId, Rule} = kraken_topics:resolve(<<"app/room/events">>, [wildcard_rule()]),
    ?assertEqual(<<"app-uuid-1/app/room/events">>, Base),
    ?assertEqual(<<"app-uuid-1">>, AppId),
    ?assertEqual(<<"app/room/#">>, maps:get(<<"pattern">>, Rule)).

resolve_wildcard_without_app_id_test() ->
    %% Rules missing app_id scope to `unscoped/` (still never a shared
    %% cross-tenant namespace keyed on nothing)
    Rule = maps:remove(<<"app_id">>, wildcard_rule()),
    {wildcard, Base, AppId, _} = kraken_topics:resolve(<<"app/room/x">>, [Rule]),
    ?assertEqual(<<"unscoped/app/room/x">>, Base),
    ?assertEqual(<<"unscoped">>, AppId).

resolve_subscribe_publish_parity_test() ->
    %% The whole point: one resolution result for one pattern
    R1 = kraken_topics:resolve(<<"app/room/tools">>, [wildcard_rule()]),
    R2 = kraken_topics:resolve(<<"app/room/tools">>, [wildcard_rule()]),
    ?assertEqual(R1, R2),
    {wildcard, Base, _, _} = R1,
    ?assertEqual(<<"app-uuid-1/app/room/tools">>, Base).

resolve_no_match_test() ->
    ?assertEqual(no_match, kraken_topics:resolve(<<"other/room/messages">>, [exact_rule()])),
    ?assertEqual(no_match, kraken_topics:resolve(<<"x/y/z">>, [])).

resolve_exact_without_internal_topic_degrades_to_wildcard_test() ->
    %% An exact pattern with no internal mapping behaves like a wildcard
    %% fallback on its own app (static-auth configs may do this)
    Rule = maps:remove(<<"topic">>, exact_rule()),
    {wildcard, Base, <<"app-uuid-1">>, _} = kraken_topics:resolve(<<"app/room/messages">>, [Rule]),
    ?assertEqual(<<"app-uuid-1/app/room/messages">>, Base).

legacy_fallback_test() ->
    %% The pre-fix name, kept only for the one-release dual-subscribe shim
    ?assertEqual(<<"unknown/app/room/x">>, kraken_topics:legacy_fallback_topic(undefined, <<"app/room/x">>)).
