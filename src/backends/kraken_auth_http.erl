%%%-------------------------------------------------------------------
%% @doc HTTP auth backend: delegate token validation to an external
%% control plane. This is the contract a hosted control plane (e.g.
%% NoLag's Titus) implements to plug into kraken.
%%
%% Requests (Bearer = backend_secret config):
%%   POST {auth_http_url}/validate          {"accessToken": "..."}
%%   POST {auth_http_url}/revalidate        {"actorTokenId": "..."}
%%   POST {auth_http_url}/check-room-access {"actorTokenId": "...", "pattern": "..."}
%%   POST {auth_http_url}/authorize-publish {"apiKey": "...", "appId": "...",
%%                                           "roomId": "...", "scopeId": "..." | null,
%%                                           "topic": "..."}
%%
%% Validate response:
%%   {"result": "allow" | "deny", "client_attrs": { ...auth attrs... }}
%% Authorize-publish response (always 200; a deny is not an HTTP error):
%%   {"allow": true, "api_key_id", "organization_id", "project_id", "app_id",
%%    "room_id", "internal_topic", "pattern", "scope_id", "scope_slug",
%%    "scope_name", "max_message_size_bytes"} |
%%   {"allow": false, "reason": "invalid_api_key" | "not_found" | "forbidden"}
%% Revalidate response:
%%   {"valid": true, ...auth attrs...} |
%%   {"valid": false, "error": "...", "disconnect_reason": "..."}
%%
%% Attr shape is documented in docs/PLUGINS.md (apps/allowed_topics/...).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_auth_http).
-behaviour(kraken_auth).

-export([validate_token/1, revalidate_token/1, check_room_access/2, authorize_publish/2]).

-define(POOL, kraken_auth_pool).
-define(DEFAULT_CHECK_ROOM_TIMEOUT_MS, 2000).

validate_token(AccessToken) ->
    Body = jsx:encode(#{<<"accessToken">> => AccessToken}),
    case request(<<"/validate">>, Body) of
        {ok, 200, ResponseBody} ->
            Response = jsx:decode(ResponseBody, [return_maps]),
            case maps:get(<<"result">>, Response, <<"deny">>) of
                <<"allow">> ->
                    Attrs = maps:get(<<"client_attrs">>, Response, #{}),
                    {ok, kraken_auth:build_auth_data(Attrs)};
                <<"deny">> ->
                    {error, <<"access_denied">>}
            end;
        {ok, StatusCode, _} ->
            kraken_log:error("[AuthHttp] validate returned status ~p", [StatusCode]),
            {error, <<"authentication_failed">>};
        {error, Reason} ->
            kraken_log:error("[AuthHttp] validate request failed: ~p", [Reason]),
            {error, <<"connection_failed">>}
    end.

revalidate_token(ActorTokenId) ->
    Body = jsx:encode(#{<<"actorTokenId">> => ActorTokenId}),
    case request(<<"/revalidate">>, Body) of
        {ok, 200, ResponseBody} ->
            Response = jsx:decode(ResponseBody, [return_maps]),
            case maps:get(<<"valid">>, Response, false) of
                true ->
                    {ok, kraken_auth:build_auth_data(Response)};
                false ->
                    Error = maps:get(<<"error">>, Response, <<"unknown">>),
                    DisconnectReason = maps:get(<<"disconnect_reason">>, Response, Error),
                    {error, DisconnectReason}
            end;
        {ok, StatusCode, _} ->
            kraken_log:error("[AuthHttp] revalidate returned status ~p", [StatusCode]),
            {retry, <<"server_error">>};
        {error, Reason} ->
            kraken_log:error("[AuthHttp] revalidate request failed: ~p", [Reason]),
            {retry, <<"connection_failed">>}
    end.

%% Scoped live room-access check for the broker's subscribe cache-miss
%% fallback. The broker hits this only when an actor subscribes to a topic
%% pattern not in its cached allowed_topics (e.g. a room created after the
%% actor connected). Returns the room's allowed_topics (control-plane shape,
%% app_id already on each entry) on allow, or {error, _} on deny/failure so
%% the caller fails closed. Uses a short timeout — it blocks the actor's
%% own connection process while in flight.
check_room_access(ActorTokenId, Pattern) ->
    Body = jsx:encode(#{
        <<"actorTokenId">> => ActorTokenId,
        <<"pattern">> => Pattern
    }),
    case request(<<"/check-room-access">>, Body, check_room_timeout()) of
        {ok, 200, ResponseBody} ->
            Response = jsx:decode(ResponseBody, [return_maps]),
            case maps:get(<<"allow">>, Response, false) of
                true ->
                    {ok, maps:get(<<"allowed_topics">>, Response, [])};
                false ->
                    {error, <<"access_denied">>}
            end;
        {ok, StatusCode, _} ->
            kraken_log:error("[AuthHttp] check-room-access returned status ~p", [StatusCode]),
            {error, <<"authentication_failed">>};
        {error, Reason} ->
            kraken_log:error("[AuthHttp] check-room-access request failed: ~p", [Reason]),
            {error, <<"connection_failed">>}
    end.

%% May this project API key publish to this target? The key goes to the
%% control plane, never the message: kraken asks about {key, app, room,
%% scope, topic} and caches the answer (kraken_auth:authorize_publish/2).
authorize_publish(ApiKey, Target) ->
    Body = jsx:encode(#{
        <<"apiKey">> => ApiKey,
        <<"appId">> => maps:get(app_id, Target),
        <<"roomId">> => maps:get(room_id, Target),
        <<"scopeId">> => case maps:get(scope_id, Target, undefined) of
            undefined -> null;
            S -> S
        end,
        <<"topic">> => maps:get(topic, Target)
    }),
    case request(<<"/authorize-publish">>, Body, check_room_timeout()) of
        {ok, 200, ResponseBody} ->
            parse_publish_grant(jsx:decode(ResponseBody, [return_maps]));
        {ok, StatusCode, _} ->
            kraken_log:error("[AuthHttp] authorize-publish returned status ~p", [StatusCode]),
            {error, unavailable};
        {error, Reason} ->
            kraken_log:error("[AuthHttp] authorize-publish request failed: ~p", [Reason]),
            {error, unavailable}
    end.

parse_publish_grant(#{<<"allow">> := true} = R) ->
    {ok, kraken_auth:publish_grant(R)};
parse_publish_grant(R) ->
    {error, {denied, maps:get(<<"reason">>, R, <<"forbidden">>)}}.

%%====================================================================
%% Internal
%%====================================================================

request(Path, Body) ->
    request(Path, Body, 5000).

request(Path, Body, Timeout) ->
    Headers = [
        {<<"content-type">>, <<"application/json">>},
        {<<"authorization">>, iolist_to_binary([<<"Bearer ">>, backend_secret()])}
    ],
    kraken_gun_client:request(?POOL, post, Path, Headers, Body, Timeout).

check_room_timeout() ->
    case application:get_env(kraken, cache_miss_fallback_timeout_ms) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_CHECK_ROOM_TIMEOUT_MS
    end.

backend_secret() ->
    case application:get_env(kraken, backend_secret) of
        {ok, S} when is_list(S), S =/= [] -> list_to_binary(S);
        {ok, S} when is_binary(S), S =/= <<>> -> S;
        _ -> <<>>
    end.
