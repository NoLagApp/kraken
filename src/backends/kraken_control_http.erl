%%%-------------------------------------------------------------------
%% @doc HTTP control backend: deliver control-plane events to a
%% configured base URL (control_http_url). Doubles as the contract a
%% hosted control plane implements.
%%
%% Requests (Bearer = backend_secret config):
%%   POST {control_http_url}/usage
%%     {"entries": [{"projectId": ..., "count": N, "totalBytes": N}]}
%%     -> 200 {"blockedProjects": ["..."]}   (optional)
%%   POST {control_http_url}/subscriptions
%%     {"changes": [ ... ]}
%%   POST {control_http_url}/webhook-failures
%%     { ...failure doc... }
%% @end
%%%-------------------------------------------------------------------
-module(kraken_control_http).
-behaviour(kraken_control).

-export([report_usage/1, report_subscription_change/1, report_webhook_failure/1]).

-define(POOL, kraken_control_pool).

report_usage(Entries) ->
    Body = jsx:encode(#{<<"entries">> => Entries}),
    case request(<<"/usage">>, Body) of
        {ok, 200, ResponseBody} ->
            try jsx:decode(ResponseBody, [return_maps]) of
                #{<<"blockedProjects">> := Blocked} when is_list(Blocked) ->
                    {ok, Blocked};
                _ ->
                    ok
            catch _:_ ->
                ok
            end;
        {ok, StatusCode, _} ->
            {error, {http_error, StatusCode}};
        {error, Reason} ->
            {error, Reason}
    end.

report_subscription_change(Changes) ->
    Body = jsx:encode(#{<<"changes">> => Changes}),
    case request(<<"/subscriptions">>, Body) of
        {ok, Status, _} when Status >= 200, Status < 300 -> ok;
        {ok, StatusCode, _} -> {error, {http_error, StatusCode}};
        {error, Reason} -> {error, Reason}
    end.

report_webhook_failure(Failure) ->
    Body = jsx:encode(Failure),
    _ = request(<<"/webhook-failures">>, Body),
    ok.

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
