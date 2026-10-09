%%%-------------------------------------------------------------------
%% @doc POST /v1/publish — publish from a server with a project API key.
%%
%%   POST /v1/publish
%%   Authorization: Bearer <project API key>
%%   {"messages": [{"appId", "roomId", "scopeId": null | "...", "topic",
%%                  "data", "filter"? | "filters"?, "qos"?}]}          (1..100)
%%
%%   200 {"messages": [{"id": "<msgId>"}]}
%%   4xx/5xx {"error": {"code", "message", "index"?}}
%%
%% Each message names its target by id, so one route serves any app, room
%% and scope the key's project owns. The key is checked against the control
%% plane per distinct target (cached); the message itself never leaves
%% kraken. The whole batch is authorized and validated before any of it is
%% published, then published in order through the same pipeline as a
%% WebSocket publish. The broker stamps each message as sent by the server
%% (fromType "server", from = the key's id). Server publishes never fire
%% trigger webhooks, so a backend replying to a webhook can't loop.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_publish_http_handler).
-behaviour(cowboy_handler).

-export([init/2]).

-define(MAX_MESSAGES, 100).
-define(PLATFORM_MAX_MESSAGE_SIZE, 921600).

init(Req0, State) ->
    Req = case cowboy_req:method(Req0) of
        <<"POST">> -> handle(Req0);
        _ -> reply_error(405, 40500, <<"method_not_allowed">>, undefined, Req0)
    end,
    {ok, Req, State}.

handle(Req0) ->
    case api_key(Req0) of
        undefined ->
            reply_error(401, 40100, <<"missing_api_key">>, undefined, Req0);
        ApiKey ->
            case read_body(Req0) of
                {ok, Body, Req1} -> handle_body(ApiKey, Body, Req1);
                {too_large, Req1} -> reply_error(413, 41300, <<"body_too_large">>, undefined, Req1)
            end
    end.

handle_body(ApiKey, Body, Req) ->
    case decode(Body) of
        {ok, Messages} ->
            case rate_limit(ApiKey, length(Messages)) of
                ok -> authorize_and_publish(ApiKey, Messages, Req);
                {error, rate_limited} ->
                    Req1 = cowboy_req:set_resp_header(<<"retry-after">>, <<"1">>, Req),
                    reply_error(429, 42910, <<"rate_limit_exceeded">>, undefined, Req1)
            end;
        {error, Reason, Index} ->
            reply_error(400, 40000, Reason, Index, Req)
    end.

authorize_and_publish(ApiKey, Messages, Req) ->
    case authorize_all(ApiKey, Messages) of
        {ok, Grants} ->
            case publisher_session() of
                {ok, Session} ->
                    Store = store_writer(),
                    Prepared = [{Msg, ctx(Grant, Session, Store)} || {Msg, Grant} <- lists:zip(Messages, Grants)],
                    case validate_all(Prepared) of
                        ok ->
                            Ids = [publish_one(Ctx, Msg) || {Msg, Ctx} <- Prepared],
                            reply(200, #{<<"messages">> => [#{<<"id">> => Id} || Id <- Ids]}, Req);
                        {error, Index, {Code, Reason, _Extra}} ->
                            reply_error(status_for(Code), Code, Reason, Index, Req)
                    end;
                {error, unavailable} ->
                    reply_error(503, 50300, <<"broker_unavailable">>, undefined, Req)
            end;
        {error, Index, {denied, <<"invalid_api_key">>}} ->
            reply_error(401, 40100, <<"invalid_api_key">>, Index, Req);
        {error, Index, {denied, <<"forbidden">>}} ->
            reply_error(403, 40300, <<"forbidden">>, Index, Req);
        {error, Index, {denied, _NotFound}} ->
            %% Not this project's, or no such app/room/scope/topic: the same
            %% answer either way, so the route can't be used to probe ids.
            reply_error(404, 42940, <<"unknown_topic">>, Index, Req);
        {error, Index, unavailable} ->
            reply_error(503, 50300, <<"control_plane_unavailable">>, Index, Req)
    end.

%% One authorization per distinct target, in message order.
authorize_all(ApiKey, Messages) ->
    authorize_all(ApiKey, Messages, 0, #{}, []).

authorize_all(_ApiKey, [], _I, _Seen, Acc) ->
    {ok, lists:reverse(Acc)};
authorize_all(ApiKey, [Msg | Rest], I, Seen, Acc) ->
    Target = target(Msg),
    Result = case maps:find(Target, Seen) of
        {ok, R} -> R;
        error -> kraken_auth:authorize_publish(ApiKey, Target)
    end,
    case Result of
        {ok, Grant} -> authorize_all(ApiKey, Rest, I + 1, Seen#{Target => Result}, [Grant | Acc]);
        {error, Reason} -> {error, I, Reason}
    end.

target(Msg) ->
    #{
        app_id => maps:get(<<"appId">>, Msg),
        room_id => maps:get(<<"roomId">>, Msg),
        scope_id => case maps:get(<<"scopeId">>, Msg, null) of
            null -> undefined;
            S -> S
        end,
        topic => maps:get(<<"topic">>, Msg)
    }.

ctx(Grant, Session, Store) ->
    Internal = maps:get(internal_topic, Grant),
    #{
        sender => #{type => server, id => case maps:get(api_key_id, Grant) of
            undefined -> <<"server">>;
            KeyId -> KeyId
        end},
        organization_id => maps:get(organization_id, Grant),
        project_id => maps:get(project_id, Grant),
        app => undefined,
        app_id => maps:get(app_id, Grant),
        room_id => maps:get(room_id, Grant),
        %% An exact room: the broker topic is its internal topic, the same
        %% one a WebSocket publisher to that room resolves to.
        broker_topic => Internal,
        internal_topic => Internal,
        pattern => maps:get(pattern, Grant),
        room_name => undefined,
        scope => maps:get(scope, Grant),
        broker_session => Session,
        echo_sender => undefined,
        store => Store,
        max_message_size => min(maps:get(max_message_size_bytes, Grant), ?PLATFORM_MAX_MESSAGE_SIZE),
        fire_webhooks => false
    }.

validate_all(Prepared) ->
    validate_all(Prepared, 0).

validate_all([], _I) ->
    ok;
validate_all([{Msg, Ctx} | Rest], I) ->
    case kraken_publish:validate(Ctx, Msg) of
        {ok, _} -> validate_all(Rest, I + 1);
        {error, Error} -> {error, I, Error}
    end.

publish_one(Ctx, Msg) ->
    {ok, Id} = kraken_publish:publish(Ctx, Msg),
    Id.

%%====================================================================
%% Request parsing
%%====================================================================

api_key(Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        undefined -> undefined;
        Header ->
            case binary:split(Header, <<" ">>) of
                [Scheme, Key] when Key =/= <<>> ->
                    case string:lowercase(Scheme) of
                        <<"bearer">> -> string:trim(Key);
                        _ -> undefined
                    end;
                _ -> undefined
            end
    end.

read_body(Req0) ->
    Max = max_body_bytes(),
    case cowboy_req:read_body(Req0, #{length => Max, period => 15000}) of
        {ok, Body, Req} -> {ok, Body, Req};
        {more, _Partial, Req} -> {too_large, Req}
    end.

decode(Body) ->
    try jsx:decode(Body, [return_maps]) of
        #{<<"messages">> := Messages} when is_list(Messages), Messages =/= [] ->
            case length(Messages) > ?MAX_MESSAGES of
                true -> {error, <<"too_many_messages (max 100)">>, undefined};
                false -> check_messages(Messages, 0)
            end;
        _ ->
            {error, <<"expected {\"messages\": [...]}">>, undefined}
    catch
        _:_ -> {error, <<"invalid_json">>, undefined}
    end.

check_messages([], _I) ->
    {ok, []};
check_messages(Messages, I0) ->
    {Errors, _} = lists:foldl(fun(Msg, {Acc, I}) ->
        case check_message(Msg) of
            ok -> {Acc, I + 1};
            {error, Reason} -> {[{Reason, I} | Acc], I + 1}
        end
    end, {[], I0}, Messages),
    case lists:reverse(Errors) of
        [] -> {ok, Messages};
        [{Reason, I} | _] -> {error, Reason, I}
    end.

check_message(#{<<"appId">> := App, <<"roomId">> := Room, <<"topic">> := Topic, <<"data">> := _} = Msg)
  when is_binary(App), App =/= <<>>, is_binary(Room), Room =/= <<>>,
       is_binary(Topic), Topic =/= <<>> ->
    case maps:get(<<"scopeId">>, Msg, null) of
        null -> ok;
        S when is_binary(S), S =/= <<>> -> ok;
        _ -> {error, <<"scopeId must be a string or null">>}
    end;
check_message(_) ->
    {error, <<"each message needs appId, roomId, topic and data">>}.

%%====================================================================
%% Helpers
%%====================================================================

rate_limit(ApiKey, N) ->
    kraken_rate_limit:allow({publish_http, crypto:hash(sha256, ApiKey)}, N, rate_limit_per_second()).

publisher_session() ->
    case kraken_publisher_pool:session() of
        {ok, _} = Ok -> Ok;
        {error, unavailable} -> {error, unavailable}
    end.

store_writer() ->
    case kraken_store:start_writer() of
        {ok, Writer} -> Writer;
        _ -> undefined
    end.

status_for(42930) -> 413;
status_for(42920) -> 429;
status_for(42960) -> 400;
status_for(42940) -> 404;
status_for(_) -> 400.

reply(Status, Body, Req) ->
    cowboy_req:reply(Status, #{<<"content-type">> => <<"application/json">>}, jsx:encode(Body), Req).

reply_error(Status, Code, Message, Index, Req) ->
    Error0 = #{<<"code">> => Code, <<"message">> => Message},
    Error = case Index of
        undefined -> Error0;
        _ -> Error0#{<<"index">> => Index}
    end,
    reply(Status, #{<<"error">> => Error}, Req).

max_body_bytes() ->
    case application:get_env(kraken, publish_http_max_body_bytes) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> 2097152
    end.

rate_limit_per_second() ->
    case application:get_env(kraken, publish_http_rate_limit) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> 200
    end.
