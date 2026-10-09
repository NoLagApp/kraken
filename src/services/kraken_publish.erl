%%%-------------------------------------------------------------------
%% @doc The one publish pipeline.
%%
%% Every way a message enters the broker — a WebSocket publish, the HTTP
%% publish route — runs through publish/2, so the checks, the envelope and
%% the side effects cannot drift between them. The caller does only what is
%% specific to how it was reached: authenticating, rate limiting, resolving
%% the topic, and turning an error into its own wire format.
%%
%% Ctx (atom keys):
%%   sender           #{type := actor | server, id := binary(), actor_type => binary()}
%%   organization_id, project_id, app_id, room_id   binary() | undefined
%%   app              the app entry from the auth data (carries webhook config) | undefined
%%   broker_topic     resolved broker topic, before any filter suffix
%%   internal_topic   room-uuid topic used for storage | undefined
%%   pattern          the display pattern (app/[scope/]room/topic)
%%   room_name        room name for webhook payloads
%%   scope            #{id, slug, name} | undefined
%%   broker_session   the broker client for this publisher
%%   echo_sender      connection id when echo=false, else undefined
%%   store            the kraken_store writer handle
%%   max_message_size bytes, packed msgpack
%%   fire_webhooks    boolean()
%%
%% Msg (binary keys, as on the wire): data, filter | filters, qos, retain.
%%
%% Envelope on the broker: #{_msgId (when recorded), _data, _from}. The broker
%% always builds it, so whatever the publisher sent sits under _data and can
%% never pose as the broker's _from.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_publish).

-export([publish/2, validate/2, sender_fields/1, unwrap/1, generate_uuid/0]).

-type publish_error() :: {Code :: integer(), Reason :: binary(), Extra :: map()}.

-spec publish(map(), map()) -> {ok, MessageId :: binary()} | {error, publish_error()}.
publish(Ctx, Msg) ->
    case validate(Ctx, Msg) of
        {ok, Checked} -> {ok, do_publish(Ctx, Msg, Checked)};
        {error, _} = Error -> Error
    end.

%% Every check publish/2 makes, without publishing — so a batch can be
%% rejected whole before any of it goes out.
-spec validate(map(), map()) -> {ok, map()} | {error, publish_error()}.
validate(Ctx, Msg) ->
    Data = maps:get(<<"data">>, Msg),
    %% kraken_msgpack, as the brokers use: msgpack:pack raises badarg on a
    %% binary that is not valid UTF-8 (an MQTT payload, or binary data from a
    %% WebSocket client), which crashed the publishing connection.
    PackedData = case kraken_msgpack:pack(Data) of
        {error, EncodeError} -> erlang:error({unencodable_data, EncodeError});
        Packed -> iolist_to_binary(Packed)
    end,
    MaxSize = maps:get(max_message_size, Ctx),
    case byte_size(PackedData) > MaxSize of
        true ->
            {error, {42930, <<"message_too_large">>, #{<<"maxSizeBytes">> => MaxSize}}};
        false ->
            case kraken_usage:is_project_blocked(maps:get(project_id, Ctx, undefined)) of
                true ->
                    {error, {42920, <<"monthly_quota_exceeded">>, #{}}};
                false ->
                    case composite_filter(Msg) of
                        {error, Reason} ->
                            {error, {42960, <<"invalid_filter">>, #{<<"detail">> => Reason}}};
                        {ok, Filter} ->
                            {ok, #{data => Data, packed => PackedData, filter => Filter}}
                    end
            end
    end.

do_publish(Ctx, Msg, #{data := Data, packed := PackedData, filter := Filter}) ->
    Sender = maps:get(sender, Ctx),
    BaseTopic = maps:get(broker_topic, Ctx),
    BrokerTopic = case Filter of
        undefined -> BaseTopic;
        _ -> <<BaseTopic/binary, "/", Filter/binary>>
    end,
    MessageId = generate_uuid(),
    Recorded = record(Ctx, MessageId, Data, PackedData),
    Envelope0 = #{<<"_data">> => Data, <<"_from">> => from_field(Sender)},
    Envelope = case Recorded of
        true -> Envelope0#{<<"_msgId">> => MessageId};
        false -> Envelope0
    end,
    QoS = case maps:get(<<"qos">>, Msg, 1) of
        N when is_integer(N), N >= 0, N =< 2 -> N;
        _ -> 1
    end,
    Retain = maps:get(<<"retain">>, Msg, false) =:= true,
    ok = kraken_broker:publish(maps:get(broker_session, Ctx), BrokerTopic, Envelope,
                               maps:get(echo_sender, Ctx, undefined), QoS, Retain),
    case maps:get(fire_webhooks, Ctx, false) of
        true -> maybe_fire_trigger(Ctx, MessageId, Data, Filter);
        false -> ok
    end,
    %% Persistent Presence: wake offline persistent subscribers in this room.
    %% Detached by construction — see the hot-path contract in kraken_presence_store.
    kraken_presence_store:wake_offline_async(maps:get(room_id, Ctx, undefined),
                                             maps:get(app_id, Ctx, undefined)),
    MessageId.

%% Usage is always counted; the message itself only when recording is on.
%% Returns whether it was recorded, which decides whether subscribers get a
%% msgId to ack and whether the message can be replayed.
record(Ctx, MessageId, Data, PackedData) ->
    ProjectId = maps:get(project_id, Ctx, undefined),
    case ProjectId of
        undefined -> ok;
        _ -> catch kraken_usage:increment(ProjectId, 1, byte_size(jsx:encode(Data)))
    end,
    case maps:get(store, Ctx, undefined) of
        undefined ->
            false;
        Store ->
            Sender = maps:get(sender, Ctx),
            Context = #{
                organization_id => maps:get(organization_id, Ctx, undefined),
                project_id => ProjectId,
                app_id => maps:get(app_id, Ctx, undefined),
                room_id => maps:get(room_id, Ctx, undefined),
                sender_type => atom_to_binary(maps:get(type, Sender), utf8)
            },
            kraken_store:log_message(Store, MessageId, Context,
                                     maps:get(internal_topic, Ctx, undefined),
                                     maps:get(pattern, Ctx), maps:get(id, Sender),
                                     PackedData, erlang:system_time(millisecond)),
            true
    end.

%% `filter` is one value used as-is; `filters` is an AND composite —
%% lowercased, sorted and joined with `|`, exactly what a subscriber's AND
%% group normalizes to. Both are validated the way subscribe validates them:
%% a `/`, `#` or `+` would publish to a different (or invalid) broker topic.
composite_filter(Msg) ->
    case {maps:get(<<"filter">>, Msg, undefined), maps:get(<<"filters">>, Msg, undefined)} of
        {F, _} when is_binary(F), F =/= <<>> ->
            case valid_filter_value(F) of
                true -> {ok, F};
                false -> {error, <<"invalid_filter_chars (/, #, +, | not allowed)">>}
            end;
        {_, Fs} when is_list(Fs), Fs =/= [] ->
            case length(Fs) > 100 of
                true ->
                    {error, <<"too_many_filters (max 100)">>};
                false ->
                    case lists:all(fun(V) -> is_binary(V) andalso valid_filter_value(V) end, Fs) of
                        true ->
                            Sorted = lists:sort([string:lowercase(V) || V <- Fs]),
                            {ok, iolist_to_binary(lists:join(<<"|">>, Sorted))};
                        false ->
                            {error, <<"invalid_filter_chars (/, #, +, | not allowed)">>}
                    end
            end;
        _ ->
            {ok, undefined}
    end.

valid_filter_value(<<>>) -> false;
valid_filter_value(V) ->
    binary:match(V, [<<"/">>, <<"#">>, <<"+">>, <<"|">>]) =:= nomatch.

%% Per-topic webhook first, app-level trigger as the fallback.
maybe_fire_trigger(Ctx, MessageId, Data, Filter) ->
    case maps:get(app, Ctx, undefined) of
        App when is_map(App) ->
            Pattern = maps:get(pattern, Ctx),
            TopicName = topic_name(Pattern),
            TopicWebhooks = maps:get(<<"topic_webhooks">>, App, #{}),
            TopicConfig = case TopicWebhooks of
                M when is_map(M) -> maps:get(TopicName, M, #{});
                _ -> #{}
            end,
            Webhook = case maps:get(<<"on_publish">>, TopicConfig, null) of
                null -> maps:get(<<"trigger_webhook">>, App, null);
                Wh -> Wh
            end,
            case Webhook of
                Config when is_map(Config) ->
                    kraken_webhooks:call_trigger(Config, #{
                        message_id => MessageId,
                        organization_id => maps:get(organization_id, Ctx, undefined),
                        project_id => maps:get(project_id, Ctx, undefined),
                        app_id => maps:get(app_id, Ctx, undefined),
                        room_id => maps:get(room_id, Ctx, undefined),
                        room_name => maps:get(room_name, Ctx, undefined),
                        topic_name => TopicName,
                        filter => Filter,
                        scope => maps:get(scope, Ctx, undefined),
                        sender => maps:get(sender, Ctx),
                        signing_secrets => maps:get(<<"webhook_signing_secrets">>, App, []),
                        data => Data
                    });
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

topic_name(Pattern) ->
    case binary:split(Pattern, <<"/">>, [global]) of
        [] -> Pattern;
        Parts -> lists:last(Parts)
    end.

from_field(#{type := Type, id := Id}) ->
    #{<<"type">> => atom_to_binary(Type, utf8), <<"id">> => Id}.

%% Split a broker payload into {MsgId, From, Payload}. Payload keeps the
%% `_sender` wrapper of an echo=false publish so the delivering connection
%% can still drop its own messages.
-spec unwrap(term()) -> {binary() | undefined, map() | undefined, term()}.
unwrap(#{<<"_sender">> := Sender, <<"data">> := Inner}) ->
    {MsgId, From, Data} = unwrap_envelope(Inner),
    {MsgId, From, #{<<"_sender">> => Sender, <<"data">> => Data}};
unwrap(Payload) ->
    unwrap_envelope(Payload).

%% Envelopes from this pipeline always carry _from; older nodes wrote
%% #{_msgId, _data} only when recording, and the raw data otherwise.
unwrap_envelope(#{<<"_data">> := Data, <<"_from">> := From} = Env) ->
    {maps:get(<<"_msgId">>, Env, undefined), From, Data};
unwrap_envelope(#{<<"_data">> := Data, <<"_msgId">> := MsgId}) ->
    {MsgId, undefined, Data};
unwrap_envelope(Data) ->
    {undefined, undefined, Data}.

%% The sender as delivered to clients: `from` (actorTokenId, or the API key
%% id for a server publish) and `fromType` ("actor" | "server").
sender_fields(#{<<"type">> := Type, <<"id">> := Id}) when is_binary(Id) ->
    #{<<"from">> => Id, <<"fromType">> => Type};
sender_fields(_) ->
    #{}.

%% UUID v4
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:strong_rand_bytes(16),
    C2 = (C band 16#0fff) bor 16#4000,
    D2 = (D band 16#3fff) bor 16#8000,
    list_to_binary(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                                  [A, B, C2, D2, E])).
