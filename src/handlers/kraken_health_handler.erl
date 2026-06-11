%%%-------------------------------------------------------------------
%% @doc Health Check Handler
%% Simple HTTP endpoint for load balancer health checks.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_health_handler).
-behaviour(cowboy_handler).

-export([init/2]).

init(Req0, State) ->
    Req = cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>},
        <<"{\"status\":\"ok\"}">>,
        Req0),
    {ok, Req, State}.
