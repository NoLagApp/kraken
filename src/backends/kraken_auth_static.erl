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
%% Dev mode: auth_allow_all=true accepts ANY token with full access to
%% every topic (INSECURE - local development only; logged loudly).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_auth_static).
-behaviour(kraken_auth).

-export([validate_token/1, revalidate_token/1]).

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
        <<"allowed_lobbies">> => maps:get(<<"allowedLobbies">>, Entry, [])
    },
    Attrs = #{
        <<"actor_token_id">> => maps:get(<<"actorTokenId">>, Entry),
        <<"organization_id">> => maps:get(<<"organizationId">>, Entry, undefined),
        <<"project_id">> => maps:get(<<"projectId">>, Entry, undefined),
        <<"actor_type">> => maps:get(<<"actorType">>, Entry, <<"user">>),
        <<"apps">> => [App],
        <<"max_connections">> => maps:get(<<"maxConnections">>, Entry, undefined),
        <<"max_message_size_bytes">> => maps:get(<<"maxMessageSizeBytes">>, Entry, undefined),
        <<"scope_slug">> => maps:get(<<"scopeSlug">>, Entry, null)
    },
    kraken_auth:build_auth_data(Attrs).

app_from_topics([T | _]) -> maps:get(<<"app_id">>, T, undefined);
app_from_topics(_) -> undefined.

%% File loading with mtime-based reload
tokens() ->
    Path = auth_file(),
    case ets:info(?FILE_CACHE, size) of
        undefined ->
            ets:new(?FILE_CACHE, [named_table, public, set, {read_concurrency, true}]);
        _ -> ok
    end,
    MTime = file_mtime(Path),
    case ets:lookup(?FILE_CACHE, tokens) of
        [{tokens, Cached, MTime}] ->
            Cached;
        _ ->
            Loaded = load_file(Path),
            ets:insert(?FILE_CACHE, {tokens, Loaded, MTime}),
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
                Decoded -> maps:get(<<"tokens">>, Decoded, #{})
            catch _:_ ->
                kraken_log:error("[AuthStatic] Failed to parse auth file ~s", [Path]),
                #{}
            end;
        {error, Reason} ->
            kraken_log:error("[AuthStatic] Cannot read auth file ~s: ~p", [Path, Reason]),
            #{}
    end.
