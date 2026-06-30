-module(kraken_acl_tests).
-include_lib("eunit/include/eunit.hrl").

%% ACL permission checks + the short-lived negative (deny) cache that the WS
%% handler's subscribe cache-miss fallback uses to throttle repeat control-
%% plane checks for a recently-denied {actor, pattern}.

rule(Pattern, Permission) ->
    #{<<"pattern">> => Pattern, <<"permission">> => Permission}.

%%====================================================================
%% can_subscribe / can_publish
%%====================================================================

can_subscribe_exact_test() ->
    Topics = [rule(<<"app/room/messages">>, <<"subscribe">>)],
    ?assert(kraken_acl:can_subscribe(<<"app/room/messages">>, Topics)),
    ?assertNot(kraken_acl:can_subscribe(<<"app/other/messages">>, Topics)).

can_subscribe_pubsub_grants_both_test() ->
    Topics = [rule(<<"app/room/messages">>, <<"pubSub">>)],
    ?assert(kraken_acl:can_subscribe(<<"app/room/messages">>, Topics)),
    ?assert(kraken_acl:can_publish(<<"app/room/messages">>, Topics)).

can_subscribe_publish_only_cannot_subscribe_test() ->
    Topics = [rule(<<"app/room/messages">>, <<"publish">>)],
    ?assertNot(kraken_acl:can_subscribe(<<"app/room/messages">>, Topics)),
    ?assert(kraken_acl:can_publish(<<"app/room/messages">>, Topics)).

can_subscribe_empty_is_deny_test() ->
    ?assertNot(kraken_acl:can_subscribe(<<"app/room/messages">>, [])).

can_subscribe_wildcard_test() ->
    Topics = [rule(<<"app/room/#">>, <<"subscribe">>)],
    ?assert(kraken_acl:can_subscribe(<<"app/room/messages">>, Topics)),
    ?assert(kraken_acl:can_subscribe(<<"app/room/a/b">>, Topics)).

%%====================================================================
%% Deny (negative) cache
%%====================================================================

deny_cache_miss_by_default_test() ->
    %% A key that was never inserted is not cached.
    ?assertNot(kraken_acl:deny_cached(<<"actor-none">>, <<"app/room/x">>)).

deny_cache_hit_within_ttl_test() ->
    application:set_env(kraken, acl_deny_cache_ttl_ms, 60000),
    Actor = <<"actor-hit">>,
    Pattern = <<"app/room/hit">>,
    ?assertNot(kraken_acl:deny_cached(Actor, Pattern)),
    kraken_acl:deny_cache_insert(Actor, Pattern),
    ?assert(kraken_acl:deny_cached(Actor, Pattern)).

deny_cache_keyed_on_actor_and_pattern_test() ->
    application:set_env(kraken, acl_deny_cache_ttl_ms, 60000),
    kraken_acl:deny_cache_insert(<<"actor-a">>, <<"app/room/k">>),
    %% Same pattern, different actor → independent entry.
    ?assertNot(kraken_acl:deny_cached(<<"actor-b">>, <<"app/room/k">>)),
    %% Same actor, different pattern → independent entry.
    ?assertNot(kraken_acl:deny_cached(<<"actor-a">>, <<"app/room/other">>)),
    ?assert(kraken_acl:deny_cached(<<"actor-a">>, <<"app/room/k">>)).

deny_cache_expires_test() ->
    %% TTL 0 → the entry is already expired on the next lookup, so a genuinely
    %% just-granted access recovers immediately instead of staying denied.
    application:set_env(kraken, acl_deny_cache_ttl_ms, 0),
    Actor = <<"actor-exp">>,
    Pattern = <<"app/room/exp">>,
    kraken_acl:deny_cache_insert(Actor, Pattern),
    ?assertNot(kraken_acl:deny_cached(Actor, Pattern)).

deny_cache_default_ttl_when_unset_test() ->
    %% With no configured TTL the default window applies (hit, not expired).
    application:unset_env(kraken, acl_deny_cache_ttl_ms),
    Actor = <<"actor-def">>,
    Pattern = <<"app/room/def">>,
    kraken_acl:deny_cache_insert(Actor, Pattern),
    ?assert(kraken_acl:deny_cached(Actor, Pattern)).
