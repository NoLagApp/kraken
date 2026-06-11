%%%-------------------------------------------------------------------
%% @doc Webhook Service
%% Handles hydration and trigger webhook calls for Kraken.
%% Hydration webhooks pre-populate state on subscription.
%% Trigger webhooks notify external systems on publish.
%% Failed webhooks are reported via the control backend.
%%
%% Customer webhook calls stay on httpc (dynamic URLs).

%% @end
%%%-------------------------------------------------------------------
-module(kraken_webhooks).

-export([call_hydration/6, call_trigger/7, report_dlq/2]).

-define(HTTP_TIMEOUT, 30000).  % 30 seconds
-define(MAX_RETRIES, 3).

%% Call hydration webhook asynchronously
%% Sends {hydration_data, Topic, Data} or {hydration_error, Topic, Error} to WsPid
call_hydration(WsPid, WebhookConfig, ActorTokenId, RoomName, TopicName, ScopeInfo) ->
    spawn(fun() -> do_hydration(WsPid, WebhookConfig, ActorTokenId, RoomName, TopicName, ScopeInfo) end),
    ok.

%% Call trigger webhook asynchronously (fire and forget)
%% Reports to DLQ on failure
call_trigger(WebhookConfig, DlqContext, ActorTokenId, RoomName, TopicName, Data, ScopeInfo) ->
    spawn(fun() -> do_trigger(WebhookConfig, DlqContext, ActorTokenId, RoomName, TopicName, Data, ScopeInfo) end),
    ok.

%% Report failed webhook to the control plane
report_dlq(Type, Details) ->
    spawn(fun() -> do_report_dlq(Type, Details) end),
    ok.

%% Internal: Execute hydration webhook (httpc - dynamic customer URL)
do_hydration(WsPid, WebhookConfig, ActorTokenId, RoomName, TopicName, ScopeInfo) ->
    Url = maps:get(<<"url">>, WebhookConfig),
    Headers = build_headers(maps:get(<<"headers">>, WebhookConfig, #{})),

    RequestBody = jsx:encode(#{
        <<"actorId">> => ActorTokenId,
        <<"roomName">> => RoomName,
        <<"topicName">> => TopicName,
        <<"scope">> => ScopeInfo
    }),

    case make_request(Url, Headers, RequestBody, ?MAX_RETRIES) of
        {ok, ResponseBody} ->
            try
                Data = jsx:decode(ResponseBody, [return_maps]),
                WsPid ! {hydration_data, TopicName, Data}
            catch
                _:_ ->
                    kraken_log:error("[Webhook] Failed to parse hydration response for ~s", [TopicName]),
                    WsPid ! {hydration_error, TopicName, <<"invalid_json">>}
            end;
        {error, Status, ErrorMsg} ->
            kraken_log:error("[Webhook] Hydration failed for ~s: ~p ~s", [TopicName, Status, ErrorMsg]),
            WsPid ! {hydration_error, TopicName, ErrorMsg}
    end.

%% Internal: Execute trigger webhook (httpc - dynamic customer URL)
do_trigger(WebhookConfig, DlqContext, ActorTokenId, RoomName, TopicName, Data, ScopeInfo) ->
    Url = maps:get(<<"url">>, WebhookConfig),
    Headers = build_headers(maps:get(<<"headers">>, WebhookConfig, #{})),

    RequestBody = jsx:encode(#{
        <<"roomName">> => RoomName,
        <<"topicName">> => TopicName,
        <<"actorId">> => ActorTokenId,
        <<"data">> => Data,
        <<"scope">> => ScopeInfo
    }),

    case make_request(Url, Headers, RequestBody, ?MAX_RETRIES) of
        {ok, _ResponseBody} ->
            ok;  % Success - do nothing
        {error, Status, ErrorMsg} ->
            kraken_log:error("[Webhook] Trigger failed for ~s/~s: ~p ~s", [RoomName, TopicName, Status, ErrorMsg]),
            %% Report to DLQ
            report_dlq(<<"trigger">>, #{
                dlq_context => DlqContext,
                webhook_url => Url,
                request_headers => maps:get(<<"headers">>, WebhookConfig, #{}),
                request_body => #{
                    <<"roomName">> => RoomName,
                    <<"topicName">> => TopicName,
                    <<"actorId">> => ActorTokenId,
                    <<"data">> => Data,
                    <<"scope">> => ScopeInfo
                },
                response_status => Status,
                error_message => ErrorMsg
            })
    end.

%% Internal: Report failed webhook through the control backend
do_report_dlq(Type, Details) ->
    DlqContext = maps:get(dlq_context, Details, #{}),
    Failure = #{
        <<"organizationId">> => maps:get(organization_id, DlqContext, <<>>),
        <<"projectId">> => maps:get(project_id, DlqContext, <<>>),
        <<"appId">> => maps:get(app_id, DlqContext, null),
        <<"roomId">> => maps:get(room_id, DlqContext, null),
        <<"type">> => Type,
        <<"webhookUrl">> => maps:get(webhook_url, Details, <<>>),
        <<"requestHeaders">> => maps:get(request_headers, Details, #{}),
        <<"requestBody">> => maps:get(request_body, Details, #{}),
        <<"responseStatus">> => maps:get(response_status, Details, null),
        <<"errorMessage">> => maps:get(error_message, Details, <<"unknown">>),
        <<"node">> => atom_to_binary(node(), utf8),
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    kraken_control:report_webhook_failure(Failure).

%% Internal: Build HTTP headers from config map (for httpc customer webhooks)
build_headers(ConfigHeaders) when is_map(ConfigHeaders) ->
    BaseHeaders = [{"Content-Type", "application/json"}],
    CustomHeaders = maps:fold(fun(K, V, Acc) ->
        [{binary_to_list(K), binary_to_list(V)} | Acc]
    end, [], ConfigHeaders),
    BaseHeaders ++ CustomHeaders;
build_headers(_) ->
    [{"Content-Type", "application/json"}].


%% Internal: Make HTTP request with retries (httpc - customer webhooks)
make_request(Url, Headers, Body, RetriesLeft) when RetriesLeft > 0 ->
    case httpc:request(post, {binary_to_list(Url), Headers, "application/json", Body},
                       [{timeout, ?HTTP_TIMEOUT}], []) of
        {ok, {{_, StatusCode, _}, _, ResponseBody}} when StatusCode >= 200, StatusCode < 300 ->
            {ok, list_to_binary(ResponseBody)};
        {ok, {{_, StatusCode, _}, _, ResponseBody}} when StatusCode >= 500 ->
            %% Server error - retry
            timer:sleep(1000 * (?MAX_RETRIES - RetriesLeft + 1)),  % Exponential backoff
            make_request(Url, Headers, Body, RetriesLeft - 1);
        {ok, {{_, StatusCode, _}, _, ResponseBody}} ->
            %% Client error (4xx) - don't retry
            {error, StatusCode, list_to_binary(ResponseBody)};
        {error, Reason} ->
            %% Connection error - retry
            timer:sleep(1000 * (?MAX_RETRIES - RetriesLeft + 1)),
            make_request(Url, Headers, Body, RetriesLeft - 1)
    end;
make_request(_Url, _Headers, _Body, 0) ->
    {error, 0, <<"max_retries_exceeded">>}.
