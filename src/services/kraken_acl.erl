%%%-------------------------------------------------------------------
%% @doc ACL Service
%% Checks topic permissions against allowed_topics from auth.
%% No HTTP calls - uses session state only.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_acl).

-export([
    can_subscribe/2,
    can_publish/2,
    matches_pattern/2
]).

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
