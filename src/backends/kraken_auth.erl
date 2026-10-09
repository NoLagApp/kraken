%%%-------------------------------------------------------------------
%% @doc Auth behaviour + dispatcher.
%%
%% An auth backend turns an access token into an auth_result map:
%%   #{actor_token_id, organization_id, project_id, project_name,
%%     actor_type, apps, allowed_topics, active_subscriptions,
%%     allowed_lobbies, max_connections, max_message_size_bytes,
%%     persistent_session, session_expiry_seconds, scope_slug,
%%     auth_expires_at}
%%
%% auth_expires_at (unix seconds | undefined) is set for short-lived
%% client tokens (customer-minted JWTs); the ws handler disconnects the
%% connection when it passes. Absent/null for opaque actor tokens.
%%
%% Built-ins: kraken_auth_static (token file, the OSS quickstart) and
%% kraken_auth_http (delegates to an external control plane over HTTP).
%%
%% The dispatcher owns the ETS token cache (30s TTL) so every backend
%% gets burst/reconnect protection for free. The cache is keyed on the
%% SHA-256 of the token: a short non-cryptographic hash let two different
%% tokens share an entry, handing one actor's grants to another.
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
%% Optional: may this project API key publish to this target? Used by the
%% HTTP publish route. Target is #{app_id, room_id, scope_id, topic}; a grant
%% is #{api_key_id, organization_id, project_id, app_id, room_id,
%% internal_topic, pattern, scope (#{id, slug, name} | undefined),
%% max_message_size_bytes}. Deny reasons: invalid_api_key | not_found |
%% forbidden. {error, unavailable} when the control plane can't be reached.
-callback authorize_publish(ApiKey :: binary(), Target :: map()) ->
    {ok, Grant :: map()} | {error, {denied, Reason :: binary()} | unavailable}.
-optional_callbacks([check_room_access/2, authorize_publish/2]).

-export([
    validate_token/1,
    revalidate_token/1,
    check_room_access/2,
    authorize_publish/2,
    find_app_for_topic/2,
    %% shared helpers for backends building auth_result maps
    flatten_topics/1, flatten_subscriptions/1, flatten_lobbies/1,
    parse_max_connections/1, parse_max_message_size/1, parse_session_expiry/1,
    build_auth_data/1,
    ensure_cache/0,
    ensure_publish_cache/0,
    publish_grant/1
]).

-define(AUTH_CACHE, kraken_auth_token_cache).
-define(CACHE_TTL_MS, 30000).
-define(PUBLISH_CACHE, kraken_publish_auth_cache).
-define(PUBLISH_ALLOW_TTL_MS, 60000).
-define(PUBLISH_DENY_TTL_MS, 5000).

backend() -> kraken:backend(auth).

validate_token(AccessToken) ->
    TokenHash = crypto:hash(sha256, AccessToken),
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

%% API-key publish authorization, cached per {key, target}. An allow is
%% kept for a minute — the longest a revoked key keeps publishing — so a busy
%% backend asks the control plane about once a minute per room, not per
%% message. Denials are kept briefly so a misconfigured client can't hammer
%% the control plane. "Can't reach the control plane" is never cached.
authorize_publish(ApiKey, Target) ->
    Key = {crypto:hash(sha256, ApiKey), Target},
    case ttl_lookup(?PUBLISH_CACHE, Key) of
        {ok, Result} ->
            Result;
        miss ->
            Backend = backend(),
            Result = case erlang:function_exported(Backend, authorize_publish, 2) of
                true -> Backend:authorize_publish(ApiKey, Target);
                false -> {error, {denied, <<"unsupported">>}}
            end,
            case Result of
                {ok, _} -> ttl_insert(?PUBLISH_CACHE, Key, Result, ?PUBLISH_ALLOW_TTL_MS);
                {error, {denied, _}} -> ttl_insert(?PUBLISH_CACHE, Key, Result, ?PUBLISH_DENY_TTL_MS);
                _ -> ok
            end,
            Result
    end.

ensure_publish_cache() ->
    ensure_table(?PUBLISH_CACHE).

%% A publish grant from its wire shape (snake_case, binary keys) — shared by
%% every backend so they can't disagree on it.
publish_grant(R) ->
    Get = fun(K) -> optional_binary(maps:get(K, R, null)) end,
    #{
        api_key_id => Get(<<"api_key_id">>),
        organization_id => Get(<<"organization_id">>),
        project_id => Get(<<"project_id">>),
        app_id => Get(<<"app_id">>),
        room_id => Get(<<"room_id">>),
        internal_topic => Get(<<"internal_topic">>),
        pattern => Get(<<"pattern">>),
        scope => case Get(<<"scope_id">>) of
            undefined -> undefined;
            ScopeId -> #{id => ScopeId, slug => Get(<<"scope_slug">>), name => Get(<<"scope_name">>)}
        end,
        max_message_size_bytes => parse_max_message_size(maps:get(<<"max_message_size_bytes">>, R, null))
    }.

%%====================================================================
%% Token cache
%%====================================================================

%% kraken_sup calls this at boot so the long-lived supervisor owns the table.
%% Created lazily instead, it belonged to whichever connection got there first
%% and vanished when that connection closed, taking every cached token with it.
ensure_cache() ->
    ensure_table(?AUTH_CACHE).

ensure_table(Name) ->
    case ets:whereis(Name) of
        undefined ->
            %% Two processes can race here on first use; the loser's ets:new
            %% raises badarg, which we swallow (the table now exists).
            try
                ets:new(Name,
                    [named_table, public, set, {read_concurrency, true},
                     {write_concurrency, true}])
            catch
                error:badarg -> ok
            end,
            ok;
        _ ->
            ok
    end.

cache_lookup(TokenHash) ->
    ttl_lookup(?AUTH_CACHE, TokenHash).

cache_insert(TokenHash, AuthData) ->
    ttl_insert(?AUTH_CACHE, TokenHash, AuthData, ?CACHE_TTL_MS).

%% A table that disappears mid-call (its owner exited) is a cache miss,
%% never a crash.
ttl_lookup(Table, Key) ->
    ensure_table(Table),
    try ets:lookup(Table, Key) of
        [{Key, Value, ExpiresAt}] ->
            case erlang:monotonic_time(millisecond) < ExpiresAt of
                true -> {ok, Value};
                false ->
                    ets:delete(Table, Key),
                    miss
            end;
        [] ->
            miss
    catch
        error:badarg -> miss
    end.

ttl_insert(Table, Key, Value, TtlMs) ->
    ensure_table(Table),
    ExpiresAt = erlang:monotonic_time(millisecond) + TtlMs,
    try ets:insert(Table, {Key, Value, ExpiresAt}) catch error:badarg -> true end.

%%====================================================================
%% Shared helpers (same semantics across all backends)
%%====================================================================

%% Build a complete auth_result from a client-attrs style map (binary
%% keys, the on-the-wire shape used by the http backend and the static
%% file). Applies flattening + defaults.
build_auth_data(Attrs) ->
    Apps = maps:get(<<"apps">>, Attrs, []),
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
        %% All three scope fields: the slug routes topics, the id and name go
        %% out in webhook payloads and presence records. Dropping id and name
        %% here sent every webhook's scope.accessScopeId out as null.
        scope_slug => optional_binary(maps:get(<<"scope_slug">>, Attrs, null)),
        scope_id => optional_binary(maps:get(<<"scope_id">>, Attrs, null)),
        scope_name => optional_binary(maps:get(<<"scope_name">>, Attrs, null)),
        auth_expires_at => parse_auth_expires_at(maps:get(<<"auth_expires_at">>, Attrs, undefined))
    }.

optional_binary(B) when is_binary(B) -> B;
optional_binary(_) -> undefined.

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

parse_auth_expires_at(N) when is_integer(N), N > 0 -> N;
parse_auth_expires_at(_) -> undefined.

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
