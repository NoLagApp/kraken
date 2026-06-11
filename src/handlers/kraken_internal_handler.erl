%%%-------------------------------------------------------------------
%% @doc Internal HTTP Handler
%% REST endpoint for backend-to-Kraken communication.
%% Provides an internal publish endpoint so Titus can publish messages
%% to EMQX without being a connected actor (used by MCP nolag_publish).
%%
%% Authentication: Bearer token matching the internal_secret from sys.config.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_internal_handler).
-behaviour(cowboy_handler).

-export([init/2]).

init(Req0, State) ->
    Method = cowboy_req:method(Req0),
    Path = cowboy_req:path(Req0),
    case {Method, Path} of
        {<<"POST">>, <<"/internal/publish">>} ->
            handle_publish(Req0, State);
        _ ->
            Req = cowboy_req:reply(404,
                #{<<"content-type">> => <<"application/json">>},
                <<"{\"error\":\"not_found\"}">>,
                Req0),
            {ok, Req, State}
    end.

handle_publish(Req0, State) ->
    case authenticate(Req0) of
        ok ->
            {ok, Body, Req1} = cowboy_req:read_body(Req0),
            case jsx:decode(Body, [return_maps]) of
                #{<<"topic">> := Topic, <<"payload">> := Payload} = Params ->
                    Retain = maps:get(<<"retain">>, Params, false),
                    Qos = maps:get(<<"qos">>, Params, 1),
                    do_publish(Topic, Payload, Retain, Qos, Req1, State);
                _ ->
                    Req = cowboy_req:reply(400,
                        #{<<"content-type">> => <<"application/json">>},
                        <<"{\"error\":\"missing_required_fields\",\"required\":[\"topic\",\"payload\"]}">>,
                        Req1),
                    {ok, Req, State}
            end;
        unauthorized ->
            Req = cowboy_req:reply(401,
                #{<<"content-type">> => <<"application/json">>},
                <<"{\"error\":\"unauthorized\"}">>,
                Req0),
            {ok, Req, State}
    end.

do_publish(Topic, Payload, Retain, Qos, Req0, State) ->
    %% Create a temporary MQTT connection, publish, and disconnect
    case kraken_broker:connect() of
        {ok, Client} ->
            %% Encode payload as MessagePack for consistency with normal publish path
            MsgpackPayload = msgpack:pack(Payload, [{pack_str, from_binary}]),
            PublishOpts = [{qos, Qos}] ++ case Retain of
                true -> [{retain, true}];
                _ -> []
            end,
            emqtt:publish(Client, Topic, MsgpackPayload, PublishOpts),
            kraken_broker:disconnect(Client),
            Req = cowboy_req:reply(200,
                #{<<"content-type">> => <<"application/json">>},
                <<"{\"success\":true}">>,
                Req0),
            {ok, Req, State};
        {error, Reason} ->
            kraken_log:error("[Internal] Publish failed to connect to EMQX: ~p", [Reason]),
            Req = cowboy_req:reply(502,
                #{<<"content-type">> => <<"application/json">>},
                <<"{\"error\":\"broker_unavailable\"}">>,
                Req0),
            {ok, Req, State}
    end.

authenticate(Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        undefined -> unauthorized;
        AuthHeader ->
            case binary:split(AuthHeader, <<" ">>) of
                [<<"Bearer">>, Token] ->
                    case get_internal_secret() of
                        Token -> ok;
                        _ -> unauthorized
                    end;
                _ -> unauthorized
            end
    end.

get_internal_secret() ->
    case application:get_env(kraken, internal_secret) of
        {ok, Secret} when is_list(Secret) -> list_to_binary(Secret);
        {ok, Secret} when is_binary(Secret) -> Secret;
        _ -> <<"kraken_internal_secret">>
    end.
