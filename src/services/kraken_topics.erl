%%%-------------------------------------------------------------------
%% @doc Unified topic resolution.
%%
%% Single source of truth for mapping a client pattern (app/room/topic)
%% to the MQTT base topic, used identically by the WS and MQTT handlers
%% for both subscribe and publish. Replaces four divergent inline
%% implementations whose fallbacks disagreed (`unknown/<pattern>` vs
%% `<app>/<raw pattern>` vs bare topic), which caused split-brain
%% blackholes and a cross-tenant `unknown/` namespace.
%%
%% Resolution order:
%%   1. exact     — pattern has an explicit allowed_topics entry with an
%%                  internal (room-UUID) topic. Normal Titus tokens.
%%   2. cached    — an auto-provisioned room mapping is in the ETS cache
%%                  (populated by ensure_room round-trips on this node).
%%   3. wildcard  — pattern only matches a wildcard rule. Deterministic
%%                  fallback `<app_id>/<effective pattern>`, where app_id
%%                  comes from the MATCHED RULE (wildcard-aware), never
%%                  the literal `unknown`. Static-auth/OSS path.
%%   4. no_match  — nothing matches. Callers reject (not_authorized) or
%%                  attempt auto-provisioning via kraken_control:ensure_room.
%%
%% Stateless: rooms are provisioned explicitly via the control-plane rooms
%% API, never created implicitly on the data path, so the broker needs no
%% room cache. Resolution reads only the connection's own allowed_topics.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_topics).

-export([resolve/2, fallback_topic/2, legacy_fallback_topic/2]).
-export([find_rule/2]).
-export([display_for/2]).

%%====================================================================
%% Resolution
%%====================================================================

%% Resolve an effective pattern against the connection's allowed topics.
%% Returns:
%%   {exact, MqttBaseTopic, RoomId, AppId}
%%   {wildcard, MqttBaseTopic, AppId, MatchedRule}
%%   no_match
-spec resolve(binary(), list()) ->
    {exact, binary(), binary() | undefined, binary()} |
    {wildcard, binary(), binary(), map()} |
    no_match.
resolve(EffectivePattern, AllowedTopics) ->
    case find_rule(EffectivePattern, AllowedTopics) of
        {exact, Rule} ->
            InternalTopic = maps:get(<<"topic">>, Rule, undefined),
            RoomId = maps:get(<<"room_id">>, Rule, undefined),
            AppId = rule_app_id(Rule),
            case InternalTopic of
                undefined ->
                    %% Exact pattern but no internal mapping configured —
                    %% treat as wildcard-style fallback on the rule's app
                    {wildcard, fallback_topic(AppId, EffectivePattern), AppId, Rule};
                _ ->
                    {exact, InternalTopic, RoomId, AppId}
            end;
        {wildcard, Rule} ->
            AppId = rule_app_id(Rule),
            {wildcard, fallback_topic(AppId, EffectivePattern), AppId, Rule};
        not_found ->
            no_match
    end.

%% Deterministic fallback MQTT base for wildcard-matched patterns.
%% Uses the EFFECTIVE pattern (scope-injected) on every path so
%% subscribe and publish always agree.
fallback_topic(AppId, EffectivePattern) ->
    <<AppId/binary, "/", EffectivePattern/binary>>.

%% Pre-fix fallback name (`unknown/<pattern>` or differently-scoped
%% variants). Exposed for the one-release dual-subscribe compat shim so
%% in-flight old sessions still reach upgraded subscribers.
legacy_fallback_topic(_AppId, EffectivePattern) ->
    <<"unknown/", EffectivePattern/binary>>.

%% Find the first matching rule, preferring exact pattern equality over
%% wildcard matches (an exact mapping must always win).
-spec find_rule(binary(), list()) -> {exact, map()} | {wildcard, map()} | not_found.
find_rule(Pattern, AllowedTopics) ->
    find_rule(Pattern, AllowedTopics, undefined).

find_rule(_Pattern, [], undefined) ->
    not_found;
find_rule(_Pattern, [], WildcardRule) ->
    {wildcard, WildcardRule};
find_rule(Pattern, [Rule | Rest], WildcardAcc) when is_map(Rule) ->
    RulePattern = maps:get(<<"pattern">>, Rule, <<>>),
    case RulePattern =:= Pattern of
        true ->
            {exact, Rule};
        false ->
            case WildcardAcc =:= undefined andalso
                 kraken_acl:matches_pattern(Pattern, RulePattern) of
                true -> find_rule(Pattern, Rest, Rule);
                false -> find_rule(Pattern, Rest, WildcardAcc)
            end
    end;
find_rule(Pattern, [_Other | Rest], WildcardAcc) ->
    find_rule(Pattern, Rest, WildcardAcc).

rule_app_id(Rule) ->
    case maps:get(<<"app_id">>, Rule, undefined) of
        AppId when is_binary(AppId), AppId =/= <<>> -> AppId;
        _ -> <<"unscoped">>
    end.

%%====================================================================
%% Reverse resolution
%%====================================================================

%% Internal (room-uuid) topic back to the display topic the SDK keys its
%% handlers on.
%%
%% The connection normally learns this from the mapping it records when it
%% subscribes, but a resumed MQTT session flushes its queued messages
%% immediately after CONNACK — before any subscribe has been made on the new
%% connection, so that mapping does not exist yet. Those frames would then be
%% forwarded under the internal topic, and the SDK, which has no handler by
%% that name, drops them silently: exactly the messages the persistent session
%% existed to keep. `allowed_topics` carries the same pairing and is available
%% from the moment the connection authenticates.
-spec display_for(binary(), list()) -> binary() | undefined.
display_for(MqttTopic, AllowedTopics) when is_binary(MqttTopic), is_list(AllowedTopics) ->
    case exact_display(MqttTopic, AllowedTopics) of
        undefined ->
            %% A filtered publish arrives as <internal>/<filter>; the rule
            %% names only the base.
            case binary:split(MqttTopic, <<"/">>, [global]) of
                Parts when length(Parts) >= 2 ->
                    Parent = join_parts(lists:droplast(Parts)),
                    exact_display(Parent, AllowedTopics);
                _ ->
                    undefined
            end;
        Display ->
            Display
    end;
display_for(_MqttTopic, _AllowedTopics) ->
    undefined.

exact_display(MqttTopic, AllowedTopics) ->
    case lists:search(
            fun(Rule) when is_map(Rule) ->
                    maps:get(<<"topic">>, Rule, undefined) =:= MqttTopic;
               (_) -> false
            end, AllowedTopics) of
        {value, Rule} ->
            case maps:get(<<"pattern">>, Rule, undefined) of
                P when is_binary(P) -> P;
                _ -> undefined
            end;
        false ->
            undefined
    end.

join_parts([]) -> <<>>;
join_parts([H | T]) ->
    lists:foldl(fun(P, Acc) -> <<Acc/binary, "/", P/binary>> end, H, T).
