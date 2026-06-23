%%%-------------------------------------------------------------------
%% @doc In-memory durable delivery store (dev/E2E backend).
%%
%% Mirrors the prod Firestore backend for local testing without Firestore.
%% Replays from the EXISTING recorded-message table owned by
%% kraken_store_ets (`kraken_store_ets_messages') — no duplicate message
%% copy, same as prod replays from the `messages' Firestore collection.
%% Requires the message store to be recording (store_backend=ets +
%% record_messages=true) so there is a backlog to replay.
%%
%% Claims use ets:insert_new/2 — the local analog of the Firestore
%% create-if-absent precondition: exactly one caller wins per
%% {GroupId, MessageId}. Cursors are a tiny per-group resume point.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_delivery_store_ets).
-behaviour(kraken_delivery_store).

-export([init/0, is_enabled/0, pending/1, claim/1, cursor_get/1, cursor_set/2, ack/1]).

%% Read-only view of the message log owned by kraken_store_ets.
-define(MESSAGES, kraken_store_ets_messages).        %% {MessageId, Doc}
-define(CLAIMS, kraken_delivery_store_ets_claims).   %% {{GroupId, MessageId}, ActorId}
-define(CURSORS, kraken_delivery_store_ets_cursors). %% {GroupId, Pos}

init() ->
    [ensure(T) || T <- [?CLAIMS, ?CURSORS]],
    ok.

ensure(Name) ->
    case ets:info(Name, size) of
        undefined -> ets:new(Name, [named_table, public, set, {read_concurrency, true}]);
        _ -> ok
    end.

is_enabled() -> true.

%% Messages for the group's room since the cursor, oldest-first, limited.
pending(Query) ->
    init(),
    RoomId = maps:get(room_id, Query, undefined),
    Since = maps:get(cursor, Query, 0),
    Limit = maps:get(limit, Query, 100),
    Msgs = case ets:info(?MESSAGES, size) of
        undefined -> [];
        _ ->
            ets:foldl(
                fun({_MsgId, Doc}, Acc) ->
                    Ts = maps:get(timestamp, Doc, 0),
                    DocRoom = maps:get(room_id, Doc, undefined),
                    RoomOk = (RoomId =:= undefined) orelse (DocRoom =:= RoomId),
                    case RoomOk andalso Ts > Since of
                        true -> [to_entry(Doc) | Acc];
                        false -> Acc
                    end
                end, [], ?MESSAGES)
    end,
    Sorted = lists:sort(
        fun(A, B) -> maps:get(timestamp, A, 0) =< maps:get(timestamp, B, 0) end, Msgs),
    {ok, lists:sublist(Sorted, Limit)}.

%% Atomic claim: first caller to insert {GroupId, MessageId} wins.
claim(Req) ->
    init(),
    Group = maps:get(group_id, Req),
    MsgId = maps:get(message_id, Req),
    Actor = maps:get(actor_id, Req, undefined),
    case ets:insert_new(?CLAIMS, {{Group, MsgId}, Actor}) of
        true -> {ok, won};
        false -> {ok, lost}
    end.

cursor_get(Key) ->
    init(),
    Group = maps:get(group_id, Key),
    case ets:lookup(?CURSORS, Group) of
        [{_, Pos}] -> {ok, Pos};
        [] -> {ok, 0}
    end.

cursor_set(Key, Pos) ->
    init(),
    Group = maps:get(group_id, Key),
    ets:insert(?CURSORS, {Group, Pos}),
    ok.

%% The claim doc is the dedup authority for LB; nothing extra to record here.
ack(_Req) -> ok.

%%====================================================================
%% Internal
%%====================================================================

%% Replay shape consumed by kraken_replay: payload is the stored msgpack
%% binary, unpacked to a map (re-sent with isReplay => true).
to_entry(Doc) ->
    Payload = maps:get(payload, Doc, <<>>),
    Data = case msgpack:unpack(Payload, [{unpack_str, as_binary}]) of
        {ok, D} -> D;
        _ -> #{}
    end,
    #{
        message_id => maps:get(message_id, Doc),
        topic => maps:get(topic, Doc, undefined),
        pattern => maps:get(pattern, Doc, undefined),
        payload => Data,
        timestamp => maps:get(timestamp, Doc, 0)
    }.
