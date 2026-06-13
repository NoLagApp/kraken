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
%% Also owns the per-node room-mapping cache for auto-provisioned rooms:
%% {AppId, RoomSlug} -> [AllowedTopicEntry]. Connections merge entries
%% lazily on first touch; the periodic auth revalidation remains the
%% cross-node backstop.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_topics).

-behaviour(gen_server).

-export([start_link/0]).
-export([resolve/2, fallback_topic/2, legacy_fallback_topic/2]).
-export([cache_room/3, cached_room/2]).
-export([find_rule/2]).
-export([init/1, handle_call/3, handle_cast/2]).

-define(TABLE, kraken_room_cache).

%%====================================================================
%% gen_server (ETS table owner)
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ?TABLE = ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    {ok, #{}}.

handle_call(_Msg, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.

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
%% Auto-provisioned room cache
%%====================================================================

%% Cache the allowed_topics entries returned by a successful
%% kraken_control:ensure_room round-trip.
-spec cache_room(binary(), binary(), [map()]) -> ok.
cache_room(AppId, RoomSlug, Entries) when is_list(Entries) ->
    ets:insert(?TABLE, {{AppId, RoomSlug}, Entries, erlang:system_time(second)}),
    ok.

%% Look up cached entries for an auto-provisioned room.
-spec cached_room(binary(), binary()) -> {ok, [map()]} | not_found.
cached_room(AppId, RoomSlug) ->
    case ets:lookup(?TABLE, {AppId, RoomSlug}) of
        [{_, Entries, _}] -> {ok, Entries};
        [] -> not_found
    end.
