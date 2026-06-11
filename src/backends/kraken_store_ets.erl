%%%-------------------------------------------------------------------
%% @doc In-memory message store backend (the default).
%%
%% Keeps recent messages + delivery records in ETS with a TTL and a
%% bounded message count, enough to power message replay on reconnect
%% for a single node / small cluster. Node-local: each node stores the
%% messages it accepted. For durable or shared history, implement
%% kraken_store against your database (see docs/PLUGINS.md).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_store_ets).
-behaviour(kraken_store).

-export([
    init/0, terminate/0, is_enabled/0,
    log_message/1, log_delivery/1, mark_delivered/3, log_event/1,
    ack_delivery/2, batch_ack_deliveries/2,
    get_replay_messages/1, get_undelivered_count/2
]).

-define(MESSAGES, kraken_store_ets_messages).      %% {MessageId, Doc}
-define(DELIVERIES, kraken_store_ets_deliveries).  %% {{MessageId, ActorId}, Doc}
-define(EVENTS, kraken_store_ets_events).          %% {Ref, Doc}

init() ->
    [ensure(T) || T <- [?MESSAGES, ?DELIVERIES, ?EVENTS]],
    ok.

ensure(Name) ->
    case ets:info(Name, size) of
        undefined -> ets:new(Name, [named_table, public, set, {read_concurrency, true}]);
        _ -> ok
    end.

terminate() -> ok.

is_enabled() -> true.

log_message(Doc) ->
    init(),
    MessageId = maps:get(message_id, Doc),
    ets:insert(?MESSAGES, {MessageId, Doc}),
    prune(),
    {ok, MessageId}.

log_delivery(Doc) ->
    init(),
    Key = {maps:get(message_id, Doc), maps:get(actor_id, Doc)},
    ets:insert(?DELIVERIES, {Key, Doc}),
    ok.

mark_delivered(MessageId, ActorId, Timestamp) ->
    init(),
    case ets:lookup(?DELIVERIES, {MessageId, ActorId}) of
        [{Key, Doc}] ->
            ets:insert(?DELIVERIES, {Key, Doc#{status => acked, acked_at => Timestamp}});
        [] ->
            ok
    end,
    ok.

log_event(Doc) ->
    init(),
    ets:insert(?EVENTS, {erlang:unique_integer([monotonic]), Doc}),
    ok.

ack_delivery(MessageId, ActorId) ->
    mark_delivered(MessageId, ActorId, erlang:system_time(millisecond)).

batch_ack_deliveries(ActorId, MessageIds) ->
    Now = erlang:system_time(millisecond),
    [mark_delivered(M, ActorId, Now) || M <- MessageIds],
    ok.

%% Replay: pending deliveries for this actor, joined to their messages,
%% oldest first (the replay service replays in order).
get_replay_messages(Options) ->
    init(),
    ActorId = maps:get(actor_id, Options, undefined),
    Limit = maps:get(limit, Options, 100),
    TtlMs = ttl_ms(),
    Now = erlang:system_time(millisecond),
    Pending = ets:foldl(
        fun({{MessageId, A}, Doc}, Acc) when A =:= ActorId ->
                case maps:get(status, Doc, pending) of
                    pending ->
                        case ets:lookup(?MESSAGES, MessageId) of
                            [{_, MsgDoc}] ->
                                Ts = maps:get(timestamp, MsgDoc, 0),
                                case Now - Ts =< TtlMs of
                                    true -> [to_replay(MsgDoc) | Acc];
                                    false -> Acc
                                end;
                            [] -> Acc
                        end;
                    _ -> Acc
                end;
           (_, Acc) -> Acc
        end, [], ?DELIVERIES),
    Sorted = lists:sort(fun(A, B) ->
        maps:get(timestamp, A, 0) =< maps:get(timestamp, B, 0)
    end, Pending),
    Limited = lists:sublist(Sorted, Limit),
    {ok, Limited, length(Limited)}.

get_undelivered_count(ActorId, _AppId) ->
    init(),
    Count = ets:foldl(
        fun({{_, A}, Doc}, Acc) when A =:= ActorId ->
                case maps:get(status, Doc, pending) of
                    pending -> Acc + 1;
                    _ -> Acc
                end;
           (_, Acc) -> Acc
        end, 0, ?DELIVERIES),
    {ok, Count}.

%%====================================================================
%% Internal
%%====================================================================

%% Shape consumed by the replay service: payload is the packed msgpack
%% binary; it gets unpacked and re-sent with isReplay => true.
to_replay(MsgDoc) ->
    Payload = maps:get(payload, MsgDoc, <<>>),
    Data = case msgpack:unpack(Payload, [{unpack_str, as_binary}]) of
        {ok, D} -> D;
        _ -> #{}
    end,
    #{
        message_id => maps:get(message_id, MsgDoc),
        topic => maps:get(topic, MsgDoc, undefined),
        pattern => maps:get(pattern, MsgDoc, undefined),
        payload => Data,
        timestamp => maps:get(timestamp, MsgDoc, 0)
    }.

ttl_ms() ->
    Seconds = case application:get_env(kraken, store_ttl_seconds, 3600) of
        N when is_integer(N), N > 0 -> N;
        _ -> 3600
    end,
    Seconds * 1000.

max_messages() ->
    case application:get_env(kraken, store_max_messages, 10000) of
        N when is_integer(N), N > 0 -> N;
        _ -> 10000
    end.

%% Bound memory: when over the cap, drop the oldest ~10%, plus their
%% delivery records.
prune() ->
    Max = max_messages(),
    case ets:info(?MESSAGES, size) of
        Size when is_integer(Size), Size > Max ->
            All = ets:foldl(fun({Id, Doc}, Acc) ->
                [{maps:get(timestamp, Doc, 0), Id} | Acc]
            end, [], ?MESSAGES),
            Sorted = lists:sort(All),
            DropN = max(Size - Max, Size div 10),
            ToDrop = lists:sublist(Sorted, DropN),
            lists:foreach(fun({_, Id}) ->
                ets:delete(?MESSAGES, Id),
                ets:match_delete(?DELIVERIES, {{Id, '_'}, '_'})
            end, ToDrop);
        _ ->
            ok
    end.
