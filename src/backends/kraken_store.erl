%%%-------------------------------------------------------------------
%% @doc Message store behaviour + dispatcher.
%%
%% A store backend persists messages, delivery records and events to
%% power message history and replay-on-reconnect. Built-ins:
%% kraken_store_ets (default, in-memory with TTL) and kraken_store_noop.
%%
%% The dispatcher exposes two surfaces:
%%  - canonical doc-based callbacks (what backends implement)
%%  - the historical writer-style arities used by connection handlers
%%    (log_message/8, log_delivery/5, mark_delivered/4, log_event/2,
%%    start_writer/0) which build docs and delegate
%%
%% Whether anything is recorded at all is governed by the
%% record_messages app env, independent of backend choice.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_store).

%% Behaviour
-callback init() -> ok | {error, term()}.
-callback terminate() -> ok.
-callback is_enabled() -> boolean().
-callback log_message(Doc :: map()) -> ok | {error, term()}.
-callback log_delivery(Doc :: map()) -> ok | {error, term()}.
-callback mark_delivered(MessageId :: binary(), ActorId :: binary(), Timestamp :: integer()) -> ok.
-callback log_event(Doc :: map()) -> ok.
-callback ack_delivery(MessageId :: binary(), ActorId :: binary()) -> ok | {error, term()}.
-callback batch_ack_deliveries(ActorId :: binary(), MessageIds :: [binary()]) -> ok | {error, term()}.
-callback get_replay_messages(Options :: map()) ->
    {ok, Messages :: [map()], Count :: integer()} | {error, term()}.
-callback get_undelivered_count(ActorId :: binary(), AppId :: binary()) ->
    {ok, Count :: integer()} | {error, term()}.

-export([
    %% lifecycle / config
    backend/0, init/0, terminate/0, is_enabled/0,
    start_writer/0, stop_writer/1,
    %% writer-style surface (used by connection handlers)
    log_message/8, log_delivery/5, mark_delivered/4, log_event/2,
    %% logger-style surface (replay + acks)
    log_message/1, log_message_with_deliveries/2,
    ack_delivery/2, batch_ack_deliveries/2,
    get_replay_messages/1, get_undelivered_count/2
]).

backend() -> kraken:backend(store).

init() ->
    Module = backend(),
    io:format("[KrakenStore] Initializing with backend: ~p~n", [Module]),
    Module:init().

terminate() ->
    (backend()):terminate().

is_enabled() ->
    recording_on() andalso (backend()):is_enabled().

recording_on() ->
    case application:get_env(kraken, record_messages, false) of
        true -> true;
        "true" -> true;
        <<"true">> -> true;
        _ -> false
    end.

%% Handler handle: enabled | undefined (mirrors firestore_writer:start_writer/0)
start_writer() ->
    case is_enabled() of
        true -> {ok, enabled};
        false -> {ok, undefined}
    end.

stop_writer(_) -> ok.

%%====================================================================
%% Writer-style surface
%%====================================================================

log_message(undefined, _, _, _, _, _, _, _) -> ok;
log_message(enabled, MessageId, Context, Topic, Pattern, SenderActorId, PackedPayload, Timestamp) ->
    TopicName = case binary:split(Pattern, <<"/">>, [global]) of
        [] -> Pattern;
        Parts -> lists:last(Parts)
    end,
    Doc = #{
        message_id => MessageId,
        organization_id => maps:get(organization_id, Context, undefined),
        project_id => maps:get(project_id, Context, undefined),
        app_id => maps:get(app_id, Context, undefined),
        room_id => maps:get(room_id, Context, undefined),
        topic => Topic,
        topic_name => TopicName,
        pattern => Pattern,
        sender_actor_id => SenderActorId,
        payload => iolist_to_binary(PackedPayload),
        payload_size => iolist_size(PackedPayload),
        timestamp => Timestamp
    },
    catch (backend()):log_message(Doc),
    ok.

log_delivery(undefined, _, _, _, _) -> ok;
log_delivery(enabled, MessageId, ActorId, Topic, Timestamp) ->
    Doc = #{
        message_id => MessageId,
        actor_id => ActorId,
        topic => Topic,
        delivered_at => Timestamp,
        status => pending
    },
    catch (backend()):log_delivery(Doc),
    ok.

mark_delivered(undefined, _, _, _) -> ok;
mark_delivered(enabled, MessageId, ActorId, Timestamp) ->
    catch (backend()):mark_delivered(MessageId, ActorId, Timestamp),
    ok.

log_event(EventType, EventData) ->
    case is_enabled() of
        true ->
            Doc = EventData#{event_type => EventType,
                             timestamp => erlang:system_time(millisecond)},
            catch (backend()):log_event(Doc),
            ok;
        false ->
            ok
    end.

%%====================================================================
%% Logger-style surface
%%====================================================================

log_message(Message) ->
    case is_enabled() of
        true -> (backend()):log_message(Message);
        false -> {ok, <<"logging_disabled">>}
    end.

log_message_with_deliveries(Message, ActorIds) ->
    case is_enabled() of
        true ->
            Backend = backend(),
            Result = Backend:log_message(Message),
            MessageId = maps:get(message_id, Message, undefined),
            Now = erlang:system_time(millisecond),
            [Backend:log_delivery(#{message_id => MessageId, actor_id => A,
                                    topic => maps:get(topic, Message, undefined),
                                    delivered_at => Now, status => pending})
             || A <- ActorIds, MessageId =/= undefined],
            Result;
        false ->
            {ok, <<"logging_disabled">>}
    end.

ack_delivery(MessageId, ActorId) ->
    case is_enabled() of
        true -> (backend()):ack_delivery(MessageId, ActorId);
        false -> ok
    end.

batch_ack_deliveries(ActorId, MessageIds) ->
    case is_enabled() of
        true -> (backend()):batch_ack_deliveries(ActorId, MessageIds);
        false -> ok
    end.

get_replay_messages(Options) ->
    case is_enabled() of
        true -> (backend()):get_replay_messages(Options);
        false -> {ok, [], 0}
    end.

get_undelivered_count(ActorId, AppId) ->
    case is_enabled() of
        true -> (backend()):get_undelivered_count(ActorId, AppId);
        false -> {ok, 0}
    end.
