%%%-------------------------------------------------------------------
%% @doc Webhook Service
%% Handles hydration and trigger webhook calls for Kraken.
%% Hydration webhooks pre-populate state on subscription.
%% Trigger webhooks notify external systems on publish.
%% Failed trigger webhooks are reported via the control backend.
%%
%% A trigger is signed when its app has a signing secret, Stripe-style:
%%
%%   NoLag-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>
%%
%% with one v1 per active secret, so a receiver keeps verifying while a
%% rotated secret is still cached by live connections. Signed triggers use the
%% versioned v2 body; apps without a secret keep the legacy body unchanged.
%%
%% Failure reports carry metadata only — never the message data or the
%% webhook's own headers. The message stays in kraken's store under its id.
%%
%% Customer webhook calls stay on httpc (dynamic URLs).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_webhooks).

-export([call_hydration/6, call_trigger/2]).
%% Exported for tests
-export([trigger_request/2, signature_header/3, backoff_ms/2]).

-define(HYDRATION_TIMEOUT, 30000).
-define(HYDRATION_ATTEMPTS, 3).

%% Call hydration webhook asynchronously
%% Sends {hydration_data, Topic, Data} or {hydration_error, Topic, Error} to WsPid
call_hydration(WsPid, WebhookConfig, ActorTokenId, RoomName, TopicName, ScopeInfo) ->
    spawn(fun() -> do_hydration(WsPid, WebhookConfig, ActorTokenId, RoomName, TopicName, ScopeInfo) end),
    ok.

%% Call a trigger webhook asynchronously (fire and forget).
%% Event (atom keys): message_id, organization_id, project_id, app_id, room_id,
%% room_name, topic_name, filter, scope (#{id, slug, name} | undefined),
%% sender (#{type, id, actor_type?}), signing_secrets ([binary()]), data.
call_trigger(WebhookConfig, Event) ->
    spawn(fun() -> do_trigger(WebhookConfig, Event) end),
    ok.

%% Internal: Execute hydration webhook (httpc - dynamic customer URL)
do_hydration(WsPid, WebhookConfig, ActorTokenId, RoomName, TopicName, ScopeInfo) ->
    Url = maps:get(<<"url">>, WebhookConfig),
    Headers = custom_headers(maps:get(<<"headers">>, WebhookConfig, #{})),

    RequestBody = jsx:encode(#{
        <<"actorId">> => ActorTokenId,
        <<"roomName">> => RoomName,
        <<"topicName">> => TopicName,
        <<"scope">> => ScopeInfo
    }),

    case post(Url, Headers, RequestBody, ?HYDRATION_ATTEMPTS, ?HYDRATION_TIMEOUT) of
        {ok, ResponseBody, _Attempts} ->
            try
                Data = jsx:decode(ResponseBody, [return_maps]),
                WsPid ! {hydration_data, TopicName, Data}
            catch
                _:_ ->
                    kraken_log:error("[Webhook] Failed to parse hydration response for ~s", [TopicName]),
                    WsPid ! {hydration_error, TopicName, <<"invalid_json">>}
            end;
        {error, Status, ErrorMsg, _Attempts} ->
            kraken_log:error("[Webhook] Hydration failed for ~s: ~p ~s", [TopicName, Status, ErrorMsg]),
            WsPid ! {hydration_error, TopicName, ErrorMsg}
    end.

%% Internal: Execute trigger webhook
do_trigger(WebhookConfig, Event) ->
    Url = maps:get(<<"url">>, WebhookConfig),
    {Headers, Body} = trigger_request(WebhookConfig, Event),
    case post(Url, Headers, Body, max_attempts(), timeout_ms()) of
        {ok, _ResponseBody, _Attempts} ->
            ok;
        {error, Status, ErrorMsg, Attempts} ->
            kraken_log:error("[Webhook] Trigger failed for ~s/~s: ~p ~s",
                [maps:get(room_id, Event, undefined), maps:get(topic_name, Event, undefined),
                 Status, ErrorMsg]),
            report_failure(Url, Status, ErrorMsg, Attempts, Event)
    end.

%% The headers and body of a trigger call: v2 + signature when the app has
%% signing secrets, the legacy body otherwise.
trigger_request(WebhookConfig, Event) ->
    Custom = custom_headers(maps:get(<<"headers">>, WebhookConfig, #{})),
    case [S || S <- maps:get(signing_secrets, Event, []), is_binary(S), S =/= <<>>] of
        [] ->
            {Custom, jsx:encode(legacy_body(Event))};
        Secrets ->
            Body = jsx:encode(v2_body(Event)),
            Timestamp = integer_to_binary(erlang:system_time(second)),
            Signed = [
                {"nolag-signature", binary_to_list(signature_header(Secrets, Timestamp, Body))},
                {"nolag-webhook-id", binary_to_list(maps:get(message_id, Event))}
            ],
            {Signed ++ Custom, Body}
    end.

signature_header(Secrets, Timestamp, Body) ->
    Signed = <<Timestamp/binary, ".", Body/binary>>,
    Sigs = [<<"v1=", (binary:encode_hex(crypto:mac(hmac, sha256, S, Signed), lowercase))/binary>>
            || S <- Secrets],
    iolist_to_binary([<<"t=">>, Timestamp, [[<<",">>, Sig] || Sig <- Sigs]]).

v2_body(Event) ->
    #{
        <<"id">> => maps:get(message_id, Event),
        <<"type">> => <<"message.published">>,
        <<"createdAt">> => erlang:system_time(millisecond),
        <<"projectId">> => null_if_undefined(maps:get(project_id, Event, undefined)),
        <<"appId">> => null_if_undefined(maps:get(app_id, Event, undefined)),
        <<"roomId">> => null_if_undefined(maps:get(room_id, Event, undefined)),
        <<"scopeId">> => case maps:get(scope, Event, undefined) of
            #{id := ScopeId} -> null_if_undefined(ScopeId);
            _ -> null
        end,
        <<"topic">> => maps:get(topic_name, Event),
        <<"filter">> => null_if_undefined(maps:get(filter, Event, undefined)),
        <<"sender">> => sender_body(maps:get(sender, Event)),
        <<"data">> => maps:get(data, Event)
    }.

sender_body(#{type := Type, id := Id} = Sender) ->
    Base = #{<<"type">> => atom_to_binary(Type, utf8), <<"id">> => Id},
    case maps:get(actor_type, Sender, undefined) of
        ActorType when is_binary(ActorType) -> Base#{<<"actorType">> => ActorType};
        _ -> Base
    end.

%% The body every unsigned webhook has always received.
legacy_body(Event) ->
    #{
        <<"roomName">> => null_if_undefined(maps:get(room_name, Event, undefined)),
        <<"topicName">> => maps:get(topic_name, Event),
        <<"actorId">> => maps:get(id, maps:get(sender, Event)),
        <<"data">> => maps:get(data, Event),
        <<"scope">> => case maps:get(scope, Event, undefined) of
            #{id := Id, slug := Slug, name := Name} ->
                #{<<"accessScopeId">> => null_if_undefined(Id),
                  <<"slug">> => null_if_undefined(Slug),
                  <<"name">> => null_if_undefined(Name)};
            _ ->
                null
        end
    }.

%% Metadata only: the id lets an operator find the message in kraken's store.
report_failure(Url, Status, ErrorMsg, Attempts, Event) ->
    Failure = #{
        <<"organizationId">> => null_if_undefined(maps:get(organization_id, Event, undefined)),
        <<"projectId">> => null_if_undefined(maps:get(project_id, Event, undefined)),
        <<"appId">> => null_if_undefined(maps:get(app_id, Event, undefined)),
        <<"roomId">> => null_if_undefined(maps:get(room_id, Event, undefined)),
        <<"msgId">> => maps:get(message_id, Event),
        <<"type">> => <<"trigger">>,
        <<"webhookUrl">> => Url,
        <<"responseStatus">> => case Status of 0 -> null; _ -> Status end,
        <<"errorMessage">> => ErrorMsg,
        <<"attempts">> => Attempts,
        <<"node">> => atom_to_binary(node(), utf8),
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    kraken_control:report_webhook_failure(Failure).

null_if_undefined(undefined) -> null;
null_if_undefined(V) -> V.

custom_headers(ConfigHeaders) when is_map(ConfigHeaders) ->
    maps:fold(fun(K, V, Acc) when is_binary(K), is_binary(V) ->
                      [{binary_to_list(K), binary_to_list(V)} | Acc];
                 (_, _, Acc) -> Acc
              end, [], ConfigHeaders);
custom_headers(_) ->
    [].

%% POST with retries. 2xx succeeds; 4xx fails at once (the receiver rejected
%% it, sending it again won't change that); 5xx and connection errors retry
%% with exponential backoff. The last real status and error are kept.
post(Url, Headers, Body, MaxAttempts, Timeout) ->
    post(Url, Headers, Body, MaxAttempts, Timeout, 1).

post(Url, Headers, Body, MaxAttempts, Timeout, Attempt) ->
    Result = httpc:request(post, {binary_to_list(Url), Headers, "application/json", Body},
                           [{timeout, Timeout}], [{body_format, binary}]),
    case Result of
        {ok, {{_, Status, _}, _, ResponseBody}} when Status >= 200, Status < 300 ->
            {ok, ResponseBody, Attempt};
        {ok, {{_, Status, _}, _, ResponseBody}} when Status >= 500 ->
            retry_or_fail(Url, Headers, Body, MaxAttempts, Timeout, Attempt,
                          Status, snippet(ResponseBody));
        {ok, {{_, Status, _}, _, ResponseBody}} ->
            {error, Status, snippet(ResponseBody), Attempt};
        {error, Reason} ->
            retry_or_fail(Url, Headers, Body, MaxAttempts, Timeout, Attempt,
                          0, iolist_to_binary(io_lib:format("~0p", [Reason])))
    end.

retry_or_fail(Url, Headers, Body, MaxAttempts, Timeout, Attempt, Status, Error)
  when Attempt < MaxAttempts ->
    timer:sleep(backoff_ms(Attempt, base_backoff_ms())),
    case post(Url, Headers, Body, MaxAttempts, Timeout, Attempt + 1) of
        {error, 0, _, Attempts} when Status =/= 0 ->
            %% A later connection error shouldn't hide the status we did get.
            {error, Status, Error, Attempts};
        Other ->
            Other
    end;
retry_or_fail(_Url, _Headers, _Body, _MaxAttempts, _Timeout, Attempt, Status, Error) ->
    {error, Status, Error, Attempt}.

%% Base * 4^(attempt-1), +/-20% jitter: ~1s, ~4s, ~16s with the default base.
backoff_ms(Attempt, Base) ->
    Delay = Base * trunc(math:pow(4, Attempt - 1)),
    Jitter = Delay div 5,
    case Jitter of
        0 -> Delay;
        _ -> Delay - Jitter + rand:uniform(2 * Jitter + 1) - 1
    end.

snippet(Body) when is_binary(Body), byte_size(Body) > 500 ->
    binary:part(Body, 0, 500);
snippet(Body) when is_binary(Body) ->
    Body;
snippet(Body) ->
    iolist_to_binary(Body).

max_attempts() -> env_int(webhook_max_attempts, 3, 1).
timeout_ms() -> env_int(webhook_timeout_ms, 30000, 1000).
base_backoff_ms() -> env_int(webhook_backoff_ms, 1000, 0).

env_int(Key, Default, Min) ->
    case application:get_env(kraken, Key) of
        {ok, N} when is_integer(N), N >= Min -> N;
        {ok, S} when is_list(S) ->
            case catch list_to_integer(S) of
                N when is_integer(N), N >= Min -> N;
                _ -> Default
            end;
        _ -> Default
    end.
