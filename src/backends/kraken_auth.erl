%%%-------------------------------------------------------------------
%% @doc Auth behaviour + dispatcher.
%%
%% An auth backend turns an access token into an auth_result map:
%%   #{actor_token_id, organization_id, project_id, project_name,
%%     actor_type, apps, allowed_topics, active_subscriptions,
%%     allowed_lobbies, max_connections, max_message_size_bytes,
%%     persistent_session, session_expiry_seconds, scope_slug}
%%
%% Built-ins: kraken_auth_static (token file, the OSS quickstart) and
%% kraken_auth_http (delegates to an external control plane over HTTP).
%%
%% The dispatcher owns the ETS token cache (30s TTL) so every backend
%% gets burst/reconnect protection for free.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_auth).

%% Behaviour
-callback validate_token(AccessToken :: binary()) ->
    {ok, AuthData :: map()} | {error, Reason :: binary()}.
-callback revalidate_token(ActorTokenId :: binary()) ->
    {ok, AuthData :: map()} | {error, Reason :: binary()} | {retry, Reason :: binary()}.
%% Optional: live, scoped room-access check for the subscribe cache-miss
%% fallback. Only the http backend implements it; backends without a control
%% plane (e.g. the static token file) simply don't, and the fallback no-ops.
-callback check_room_access(ActorTokenId :: binary(), Pattern :: binary()) ->
    {ok, AllowedTopics :: list()} | {error, Reason :: binary()}.
-optional_callbacks([check_room_access/2]).

-export([
    validate_token/1,
    revalidate_token/1,
    check_room_access/2,
    find_app_for_topic/2,
    %% shared helpers for backends building auth_result maps
    flatten_topics/1, flatten_subscriptions/1, flatten_lobbies/1,
    parse_max_connections/1, parse_max_message_size/1, parse_session_expiry/1,
    build_auth_data/1
]).

-define(AUTH_CACHE, kraken_auth_token_cache).
-define(CACHE_TTL_MS, 30000).

backend() -> kraken:backend(auth).

validate_token(AccessToken) ->
    TokenHash = erlang:phash2(AccessToken),
    case cache_lookup(TokenHash) of
        {ok, CachedAuthData} ->
            {ok, CachedAuthData};
        miss ->
            case (backend()):validate_token(AccessToken) of
                {ok, AuthData} = Result ->
                    cache_insert(TokenHash, AuthData),
                    Result;
                Error ->
                    Error
            end
    end.

revalidate_token(ActorTokenId) ->
    (backend()):revalidate_token(ActorTokenId).

%% Live room-access check (subscribe cache-miss fallback). Not cached — the
%% whole point is to read fresh control-plane state for a room that wasn't
%% known at connect. Backends that don't implement it (no control plane)
%% return {error, unsupported} so the caller fails closed.
check_room_access(ActorTokenId, Pattern) ->
    Backend = backend(),
    case erlang:function_exported(Backend, check_room_access, 2) of
        true -> Backend:check_room_access(ActorTokenId, Pattern);
        false -> {error, <<"unsupported">>}
    end.

%%====================================================================
%% Token cache
%%====================================================================

ensure_cache() ->
    case ets:info(?AUTH_CACHE, size) of
        undefined ->
            ets:new(?AUTH_CACHE, [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ok
    end.

cache_lookup(TokenHash) ->
    ensure_cache(),
    case ets:lookup(?AUTH_CACHE, TokenHash) of
        [{TokenHash, AuthData, ExpiresAt}] ->
            Now = erlang:monotonic_time(millisecond),
            case Now < ExpiresAt of
                true -> {ok, AuthData};
                false ->
                    ets:delete(?AUTH_CACHE, TokenHash),
                    miss
            end;
        [] ->
            miss
    end.

cache_insert(TokenHash, AuthData) ->
    ensure_cache(),
    ExpiresAt = erlang:monotonic_time(millisecond) + ?CACHE_TTL_MS,
    ets:insert(?AUTH_CACHE, {TokenHash, AuthData, ExpiresAt}).

%%====================================================================
%% Shared helpers (same semantics across all backends)
%%====================================================================

%% Build a complete auth_result from a client-attrs style map (binary
%% keys, the on-the-wire shape used by the http backend and the static
%% file). Applies flattening + defaults.
build_auth_data(Attrs) ->
    Apps = maps:get(<<"apps">>, Attrs, []),
    RawScopeSlug = maps:get(<<"scope_slug">>, Attrs, null),
    ScopeSlug = case RawScopeSlug of
        null -> undefined;
        undefined -> undefined;
        Slug when is_binary(Slug) -> Slug
    end,
    #{
        actor_token_id => maps:get(<<"actor_token_id">>, Attrs),
        organization_id => maps:get(<<"organization_id">>, Attrs, undefined),
        project_id => maps:get(<<"project_id">>, Attrs, undefined),
        project_name => maps:get(<<"project_name">>, Attrs, undefined),
        actor_type => maps:get(<<"actor_type">>, Attrs, <<"user">>),
        apps => Apps,
        allowed_topics => flatten_topics(Apps),
        active_subscriptions => flatten_subscriptions(Apps),
        allowed_lobbies => flatten_lobbies(Apps),
        max_connections => parse_max_connections(maps:get(<<"max_connections">>, Attrs, undefined)),
        max_message_size_bytes => parse_max_message_size(maps:get(<<"max_message_size_bytes">>, Attrs, undefined)),
        persistent_session => maps:get(<<"persistent_session">>, Attrs, false),
        session_expiry_seconds => parse_session_expiry(maps:get(<<"session_expiry_seconds">>, Attrs, 0)),
        scope_slug => ScopeSlug
    }.

flatten_topics(Apps) ->
    lists:flatmap(fun(App) ->
        AppId = maps:get(<<"app_id">>, App, undefined),
        AppName = maps:get(<<"app_name">>, App, undefined),
        Topics = maps:get(<<"allowed_topics">>, App, []),
        lists:map(fun(Topic) ->
            Topic#{<<"app_id">> => AppId, <<"app_name">> => AppName}
        end, Topics)
    end, Apps).

flatten_subscriptions(Apps) ->
    lists:flatmap(fun(App) ->
        maps:get(<<"active_subscriptions">>, App, [])
    end, Apps).

flatten_lobbies(Apps) ->
    lists:flatmap(fun(App) ->
        Lobbies = maps:get(<<"allowed_lobbies">>, App, []),
        lists:filtermap(fun(L) ->
            case {maps:get(<<"lobby_slug">>, L, undefined),
                  maps:get(<<"lobby_id">>, L, undefined)} of
                {undefined, _} -> false;
                {_, undefined} -> false;
                {Slug, LobbyId} -> {true, {Slug, LobbyId}}
            end
        end, Lobbies)
    end, Apps).

parse_max_connections(null) -> unlimited;
parse_max_connections(undefined) -> unlimited;
parse_max_connections(N) when is_integer(N) -> N;
parse_max_connections(_) -> unlimited.

parse_max_message_size(null) -> default_max_message_size();
parse_max_message_size(undefined) -> default_max_message_size();
parse_max_message_size(N) when is_integer(N), N > 0 -> N;
parse_max_message_size(_) -> default_max_message_size().

default_max_message_size() ->
    case application:get_env(kraken, max_message_size, 921600) of
        N when is_integer(N), N > 0 -> N;
        _ -> 921600
    end.

parse_session_expiry(null) -> 0;
parse_session_expiry(undefined) -> 0;
parse_session_expiry(N) when is_integer(N), N >= 0 -> N;
parse_session_expiry(_) -> 0.

%% Find the app that owns a topic (by exact pattern membership)
find_app_for_topic(_Pattern, []) ->
    undefined;
find_app_for_topic(Pattern, [App | Rest]) ->
    Topics = maps:get(<<"allowed_topics">>, App, []),
    case lists:any(fun(T) ->
        maps:get(<<"pattern">>, T, <<>>) =:= Pattern
    end, Topics) of
        true -> App;
        false -> find_app_for_topic(Pattern, Rest)
    end.
