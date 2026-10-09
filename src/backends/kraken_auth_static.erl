%%%-------------------------------------------------------------------
%% @doc Static-file auth backend (the OSS quickstart).
%%
%% Reads tokens from a JSON file (env AUTH_FILE / app env auth_file):
%%   {"tokens": {"<token>": {"actorTokenId": ..., "projectId": ...,
%%                           "allowedTopics": [...], "rateLimit": 50}}}
%%
%% Each token entry is normalized into the same auth_result shape the
%% http backend produces. The file is reloaded when its mtime changes.
%%
%% Project API keys for the HTTP publish route live beside the tokens:
%%   {"apiKeys": {"<key>": {"apiKeyId": ..., "projectId": ..., "organizationId": ...,
%%                          "appSlug": ..., "grants": [{"appId", "roomId", "roomSlug"?,
%%                          "scopeId"?, "scopeSlug"?, "topics": ["messages", ...]}]}}}
%% The internal topic follows the control plane's convention,
%% [scopeId/]roomId/topic, so a token's allowedTopics can name it.
%%
%% Dev mode: auth_allow_all=true accepts ANY token with full access to
%% every topic (INSECURE - local development only; logged loudly).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_auth_static).
-behaviour(kraken_auth).

-export([validate_token/1, revalidate_token/1, authorize_publish/2, ensure_file_cache/0]).

-define(FILE_CACHE, kraken_auth_static_file).

validate_token(AccessToken) ->
    case allow_all() of
        true ->
            kraken_log:info("[AuthStatic] AUTH_ALLOW_ALL active - accepting token (dev mode, INSECURE)", []),
            {ok, allow_all_auth(AccessToken)};
        false ->
            case lookup(AccessToken) of
                {ok, Entry} -> {ok, to_auth_data(Entry)};
                not_found -> {error, <<"access_denied">>}
            end
    end.

%% Static tokens don't expire server-side; revalidation re-reads the
%% file so revoking a token (deleting its entry) disconnects actors
%% within the revalidation interval.
revalidate_token(ActorTokenId) ->
    case allow_all() of
        true ->
            {ok, allow_all_auth(ActorTokenId)};
        false ->
            case find_by_actor(ActorTokenId) of
                {ok, Entry} -> {ok, to_auth_data(Entry)};
                not_found -> {error, <<"token_revoked">>}
            end
    end.

authorize_publish(ApiKey, Target) ->
    Keys = maps:get(<<"apiKeys">>, auth_file_contents(), #{}),
    case maps:get(ApiKey, Keys, undefined) of
        Entry when is_map(Entry) ->
            AppId = maps:get(app_id, Target),
            RoomId = maps:get(room_id, Target),
            ScopeId = maps:get(scope_id, Target, undefined),
            Topic = maps:get(topic, Target),
            Matches = [G || G <- maps:get(<<"grants">>, Entry, []),
                            maps:get(<<"appId">>, G, undefined) =:= AppId,
                            maps:get(<<"roomId">>, G, undefined) =:= RoomId,
                            optional(maps:get(<<"scopeId">>, G, null)) =:= ScopeId,
                            lists:member(Topic, maps:get(<<"topics">>, G, []))],
            case Matches of
                [Grant | _] -> {ok, kraken_auth:publish_grant(grant_attrs(Entry, Grant, Topic))};
                [] -> {error, {denied, <<"not_found">>}}
            end;
        _ ->
            {error, {denied, <<"invalid_api_key">>}}
    end.

grant_attrs(Entry, Grant, Topic) ->
    RoomId = maps:get(<<"roomId">>, Grant),
    ScopeId = optional(maps:get(<<"scopeId">>, Grant, null)),
    AppSlug = maps:get(<<"appSlug">>, Entry, maps:get(<<"appId">>, Grant)),
    RoomSlug = maps:get(<<"roomSlug">>, Grant, RoomId),
    {Internal, Pattern} = case ScopeId of
        undefined ->
            {<<RoomId/binary, "/", Topic/binary>>,
             <<AppSlug/binary, "/", RoomSlug/binary, "/", Topic/binary>>};
        _ ->
            ScopeSlug = maps:get(<<"scopeSlug">>, Grant, ScopeId),
            {<<ScopeId/binary, "/", RoomId/binary, "/", Topic/binary>>,
             <<AppSlug/binary, "/", ScopeSlug/binary, "/", RoomSlug/binary, "/", Topic/binary>>}
    end,
    #{
        <<"api_key_id">> => maps:get(<<"apiKeyId">>, Entry, null),
        <<"organization_id">> => maps:get(<<"organizationId">>, Entry, null),
        <<"project_id">> => maps:get(<<"projectId">>, Entry, null),
        <<"app_id">> => maps:get(<<"appId">>, Grant),
        <<"room_id">> => RoomId,
        <<"internal_topic">> => Internal,
        <<"pattern">> => Pattern,
        <<"scope_id">> => case ScopeId of undefined -> null; _ -> ScopeId end,
        <<"scope_slug">> => maps:get(<<"scopeSlug">>, Grant, null),
        <<"scope_name">> => maps:get(<<"scopeName">>, Grant, null),
        <<"max_message_size_bytes">> => maps:get(<<"maxMessageSizeBytes">>, Entry, null)
    }.

optional(B) when is_binary(B) -> B;
optional(_) -> undefined.

%%====================================================================
%% Internal
%%====================================================================

allow_all() ->
    case application:get_env(kraken, auth_allow_all, false) of
        true -> true;
        "true" -> true;
        <<"true">> -> true;
        _ -> false
    end.

allow_all_auth(Token) ->
    Id = case Token of
        T when is_binary(T), byte_size(T) > 0 -> T;
        _ -> <<"dev-actor">>
    end,
    Attrs = #{
        <<"actor_token_id">> => Id,
        <<"organization_id">> => <<"dev-org">>,
        <<"project_id">> => <<"dev-project">>,
        <<"actor_type">> => <<"user">>,
        <<"apps">> => [#{
            <<"app_id">> => <<"dev-app">>,
            <<"app_name">> => <<"dev">>,
            <<"allowed_topics">> => [#{
                <<"pattern">> => <<"#">>,
                <<"permission">> => <<"pubSub">>
            }]
        }]
    },
    kraken_auth:build_auth_data(Attrs).

lookup(AccessToken) ->
    Tokens = tokens(),
    case maps:get(AccessToken, Tokens, undefined) of
        undefined -> not_found;
        Entry -> {ok, Entry}
    end.

find_by_actor(ActorTokenId) ->
    Tokens = tokens(),
    Found = maps:fold(fun(_Tok, Entry, Acc) ->
        case Acc of
            not_found ->
                case maps:get(<<"actorTokenId">>, Entry, undefined) of
                    ActorTokenId -> {ok, Entry};
                    _ -> not_found
                end;
            _ -> Acc
        end
    end, not_found, Tokens),
    Found.

%% Normalize a token entry (camelCase JSON) into client-attrs shape and
%% run it through the shared builder.
to_auth_data(Entry) ->
    AllowedTopics = maps:get(<<"allowedTopics">>, Entry, []),
    App = #{
        <<"app_id">> => maps:get(<<"appId">>, Entry, app_from_topics(AllowedTopics)),
        <<"app_name">> => maps:get(<<"appName">>, Entry, undefined),
        <<"allowed_topics">> => AllowedTopics,
        <<"active_subscriptions">> => maps:get(<<"activeSubscriptions">>, Entry, []),
        <<"allowed_lobbies">> => maps:get(<<"allowedLobbies">>, Entry, []),
        %% Webhook config, in the control plane's wire shape, so a token file
        %% can exercise trigger webhooks too.
        <<"trigger_webhook">> => maps:get(<<"triggerWebhook">>, Entry, null),
        <<"topic_webhooks">> => maps:get(<<"topicWebhooks">>, Entry, #{}),
        <<"webhook_signing_secrets">> => maps:get(<<"webhookSigningSecrets">>, Entry, [])
    },
    Attrs = #{
        <<"actor_token_id">> => maps:get(<<"actorTokenId">>, Entry),
        <<"organization_id">> => maps:get(<<"organizationId">>, Entry, undefined),
        <<"project_id">> => maps:get(<<"projectId">>, Entry, undefined),
        <<"actor_type">> => maps:get(<<"actorType">>, Entry, <<"user">>),
        <<"apps">> => [App],
        <<"max_connections">> => maps:get(<<"maxConnections">>, Entry, undefined),
        <<"max_message_size_bytes">> => maps:get(<<"maxMessageSizeBytes">>, Entry, undefined),
        <<"scope_slug">> => maps:get(<<"scopeSlug">>, Entry, null),
        <<"scope_id">> => maps:get(<<"scopeId">>, Entry, null),
        <<"scope_name">> => maps:get(<<"scopeName">>, Entry, null)
    },
    kraken_auth:build_auth_data(Attrs).

app_from_topics([T | _]) -> maps:get(<<"app_id">>, T, undefined);
app_from_topics(_) -> undefined.

%% kraken_sup calls this at boot so the table outlives any one connection.
ensure_file_cache() ->
    case ets:whereis(?FILE_CACHE) of
        undefined ->
            try
                ets:new(?FILE_CACHE, [named_table, public, set, {read_concurrency, true}])
            catch
                error:badarg -> ok
            end,
            ok;
        _ ->
            ok
    end.

%% File loading with mtime-based reload
tokens() ->
    maps:get(<<"tokens">>, auth_file_contents(), #{}).

auth_file_contents() ->
    Path = auth_file(),
    ensure_file_cache(),
    MTime = file_mtime(Path),
    case ets:lookup(?FILE_CACHE, file) of
        [{file, Cached, MTime}] ->
            Cached;
        _ ->
            Loaded = load_file(Path),
            ets:insert(?FILE_CACHE, {file, Loaded, MTime}),
            Loaded
    end.

auth_file() ->
    case application:get_env(kraken, auth_file) of
        {ok, Path} when is_list(Path), Path =/= "" -> Path;
        {ok, Path} when is_binary(Path), Path =/= <<>> -> binary_to_list(Path);
        _ -> "examples/auth.json"
    end.

file_mtime(Path) ->
    case file:read_file_info(Path) of
        {ok, Info} -> element(6, Info);  %% #file_info.mtime
        _ -> undefined
    end.

load_file(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            try jsx:decode(Bin, [return_maps]) of
                Decoded when is_map(Decoded) -> Decoded;
                _ -> #{}
            catch _:_ ->
                kraken_log:error("[AuthStatic] Failed to parse auth file ~s", [Path]),
                #{}
            end;
        {error, Reason} ->
            kraken_log:error("[AuthStatic] Cannot read auth file ~s: ~p", [Path, Reason]),
            #{}
    end.
