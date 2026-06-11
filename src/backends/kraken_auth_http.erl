%%%-------------------------------------------------------------------
%% @doc HTTP auth backend: delegate token validation to an external
%% control plane. This is the contract a hosted control plane (e.g.
%% NoLag's Titus) implements to plug into kraken.
%%
%% Requests (Bearer = backend_secret config):
%%   POST {auth_http_url}/validate     {"accessToken": "..."}
%%   POST {auth_http_url}/revalidate   {"actorTokenId": "..."}
%%
%% Validate response:
%%   {"result": "allow" | "deny", "client_attrs": { ...auth attrs... }}
%% Revalidate response:
%%   {"valid": true, ...auth attrs...} |
%%   {"valid": false, "error": "...", "disconnect_reason": "..."}
%%
%% Attr shape is documented in docs/PLUGINS.md (apps/allowed_topics/...).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_auth_http).
-behaviour(kraken_auth).

-export([validate_token/1, revalidate_token/1]).

-define(POOL, kraken_auth_pool).

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

%%====================================================================
%% Internal
%%====================================================================

request(Path, Body) ->
    Headers = [
        {<<"content-type">>, <<"application/json">>},
        {<<"authorization">>, iolist_to_binary([<<"Bearer ">>, backend_secret()])}
    ],
    kraken_gun_client:request(?POOL, post, Path, Headers, Body, 5000).

backend_secret() ->
    case application:get_env(kraken, backend_secret) of
        {ok, S} when is_list(S), S =/= [] -> list_to_binary(S);
        {ok, S} when is_binary(S), S =/= <<>> -> S;
        _ -> <<>>
    end.
