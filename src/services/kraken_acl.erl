%%%-------------------------------------------------------------------
%% @doc ACL Service
%% Checks topic permissions against allowed_topics from auth.
%% The permission check itself makes no HTTP calls — it uses session state
%% only. This module also owns the short-lived negative (deny) cache used by
%% the WS handler's subscribe cache-miss fallback to throttle repeat control-
%% plane checks for a recently-denied {actor, pattern}.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_acl).

-export([
    can_subscribe/2,
    can_publish/2,
    matches_pattern/2,
    deny_cached/2,
    deny_cache_insert/2
]).

-define(DENY_CACHE, kraken_acl_deny_cache).
-define(DEFAULT_DENY_TTL_MS, 5000).

%% Check if client can subscribe to topic
can_subscribe(Topic, AllowedTopics) ->
    check_permission(Topic, AllowedTopics, [<<"subscribe">>, <<"pubSub">>]).

%% Check if client can publish to topic
can_publish(Topic, AllowedTopics) ->
    check_permission(Topic, AllowedTopics, [<<"publish">>, <<"pubSub">>]).

%%====================================================================
%% Internal functions
%%====================================================================

check_permission(_Topic, [], _AllowedPermissions) ->
    %% No allowed topics means no access
    false;
check_permission(Topic, AllowedTopics, AllowedPermissions) ->
    lists:any(fun(TopicRule) ->
        Pattern = maps:get(<<"pattern">>, TopicRule, <<>>),
        Permission = maps:get(<<"permission">>, TopicRule, <<>>),
        matches_pattern(Topic, Pattern) andalso
        lists:member(Permission, AllowedPermissions)
    end, AllowedTopics).

%% Match topic against MQTT-style pattern
%% + matches single level, # matches multiple levels
matches_pattern(Topic, Pattern) ->
    TopicParts = binary:split(Topic, <<"/">>, [global]),
    PatternParts = binary:split(Pattern, <<"/">>, [global]),
    match_parts(TopicParts, PatternParts).

match_parts([], []) ->
    true;
match_parts(_, [<<"#">>]) ->
    %% # matches everything remaining
    true;
match_parts([_T | TRest], [<<"+">> | PRest]) ->
    %% + matches single level
    match_parts(TRest, PRest);
match_parts([T | TRest], [T | PRest]) ->
    %% Exact match
    match_parts(TRest, PRest);
match_parts(_, _) ->
    false.

%%====================================================================
%% Negative (deny) cache — subscribe cache-miss fallback
%%
%% After a control-plane room-access check denies (or errors) for a
%% {ActorTokenId, Pattern}, remember it briefly so a retrying client can't
%% hammer the control plane. Short TTL so a genuinely just-granted access
%% recovers quickly on the next attempt. Mirrors the kraken_auth token cache.
%%====================================================================

%% True if this {actor, pattern} was denied within the TTL window.
deny_cached(ActorTokenId, Pattern) ->
    ensure_deny_cache(),
    Key = {ActorTokenId, Pattern},
    case ets:lookup(?DENY_CACHE, Key) of
        [{Key, ExpiresAt}] ->
            Now = erlang:monotonic_time(millisecond),
            case Now < ExpiresAt of
                true -> true;
                false ->
                    ets:delete(?DENY_CACHE, Key),
                    false
            end;
        [] ->
            false
    end.

deny_cache_insert(ActorTokenId, Pattern) ->
    ensure_deny_cache(),
    ExpiresAt = erlang:monotonic_time(millisecond) + deny_ttl_ms(),
    ets:insert(?DENY_CACHE, {{ActorTokenId, Pattern}, ExpiresAt}).

ensure_deny_cache() ->
    case ets:whereis(?DENY_CACHE) of
        undefined ->
            %% Two processes can race here on first use; the loser's ets:new
            %% raises badarg, which we swallow (the table now exists).
            try
                ets:new(?DENY_CACHE,
                    [named_table, public, set, {read_concurrency, true},
                     {write_concurrency, true}])
            catch
                error:badarg -> ok
            end,
            ok;
        _ ->
            ok
    end.

deny_ttl_ms() ->
    case application:get_env(kraken, acl_deny_cache_ttl_ms) of
        {ok, N} when is_integer(N), N >= 0 -> N;
        _ -> ?DEFAULT_DENY_TTL_MS
    end.
