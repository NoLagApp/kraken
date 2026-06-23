%%%-------------------------------------------------------------------
%% @doc Replay service — durable, claim-based replay for LOAD-BALANCED
%% workers that have scaled to zero.
%%
%% A task published to a shared subscription `$share/<group>/<topic>'
%% whose group is all-offline is dropped by EMQX. When a member of the
%% group reconnects (wake-triggered), the ws_handler starts a replay:
%%
%% 1. read the group's cursor (resume point)
%% 2. query kraken_delivery_store:pending/1 for messages in the group's
%%    room since the cursor (replays from the EXISTING message store —
%%    no duplicate copy)
%% 3. atomically CLAIM each candidate (kraken_delivery_store:claim/1);
%%    only the winning member streams + processes it (exactly-one)
%% 4. stream replayStart -> claimed messages (isReplay=true) -> replayEnd
%% 5. advance the group cursor and signal {replay_complete} so the
%%    ws_handler flushes any live messages buffered during replay
%%    (deduped against the replayed ids).
%%
%% Buffering of live messages during replay is owned by kraken_ws_handler
%% (#state.replay_buffer + forward_message_with_filter); this module only
%% drives the query/claim/stream and reports status back to the parent.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_replay).

-export([start_replay/4]).

%% Context map: #{group_id, room_id, limit?}
-spec start_replay(
    ActorId :: binary(),
    AppId :: binary(),
    Ctx :: map(),
    WsPid :: pid()
) -> {ok, started}.
start_replay(ActorId, AppId, Ctx, WsPid) ->
    Parent = self(),
    spawn(fun() -> do_replay(ActorId, AppId, Ctx, WsPid, Parent) end),
    {ok, started}.

%%====================================================================
%% Internal
%%====================================================================

do_replay(ActorId, AppId, Ctx, WsPid, Parent) ->
    GroupId = maps:get(group_id, Ctx),
    RoomId = maps:get(room_id, Ctx, undefined),
    Limit = maps:get(limit, Ctx, 100),
    Cursor = cursor_get(AppId, GroupId),
    case kraken_delivery_store:pending(#{
            app_id => AppId, room_id => RoomId, group_id => GroupId,
            cursor => Cursor, limit => Limit}) of
        {ok, Candidates} ->
            Parent ! {update_replay_status, ActorId, replaying},
            %% Claim each (exactly-one per group). Keep winners in order;
            %% advance the cursor past every candidate we examined (won or
            %% lost — a lost one was handled by another member).
            {WonRev, ReplayedIds, MaxTs} = lists:foldl(
                fun(Entry, {AccMsgs, AccIds, AccTs}) ->
                    MsgId = maps:get(message_id, Entry, undefined),
                    Ts = maps:get(timestamp, Entry, 0),
                    NewTs = max(Ts, AccTs),
                    case claim(AppId, GroupId, MsgId, ActorId) of
                        won  -> {[Entry | AccMsgs], add_id(MsgId, AccIds), NewTs};
                        lost -> {AccMsgs, AccIds, NewTs}
                    end
                end, {[], [], Cursor}, Candidates),
            Won = lists:reverse(WonRev),
            Parent ! {update_replayed_ids, ActorId, ReplayedIds},
            send(WsPid, #{<<"type">> => <<"replayStart">>, <<"count">> => length(Won)}),
            lists:foreach(fun(Entry) -> send(WsPid, to_frame(Entry)) end, Won),
            send(WsPid, #{<<"type">> => <<"replayEnd">>, <<"replayed">> => length(Won)}),
            catch kraken_delivery_store:cursor_set(#{app_id => AppId, group_id => GroupId}, MaxTs),
            Parent ! {replay_complete, ActorId, ReplayedIds};
        {error, Reason} ->
            kraken_log:error("[Replay] pending failed for group ~s: ~p", [GroupId, Reason]),
            send(WsPid, #{<<"type">> => <<"replayStart">>, <<"count">> => 0}),
            send(WsPid, #{<<"type">> => <<"replayEnd">>, <<"replayed">> => 0}),
            Parent ! {replay_complete, ActorId, []}
    end.

%% Claim a message for this group member. On backend error, fall back to
%% delivering (at-least-once) — duplicates are caught by consumer-side
%% idempotency, which is safer than silently dropping a task.
claim(AppId, GroupId, MsgId, ActorId) ->
    case catch kraken_delivery_store:claim(#{
            app_id => AppId, group_id => GroupId,
            message_id => MsgId, actor_id => ActorId}) of
        {ok, won} -> won;
        {ok, lost} -> lost;
        Other ->
            kraken_log:error("[Replay] claim error for ~s/~s: ~p", [GroupId, MsgId, Other]),
            won
    end.

cursor_get(AppId, GroupId) ->
    case catch kraken_delivery_store:cursor_get(#{app_id => AppId, group_id => GroupId}) of
        {ok, Pos} when is_integer(Pos) -> Pos;
        _ -> 0
    end.

add_id(undefined, Acc) -> Acc;
add_id(MsgId, Acc) -> [MsgId | Acc].

%% Wire frame for a replayed message — same shape as a normally-forwarded
%% message (kraken_ws_handler:forward_message_with_id) + isReplay.
to_frame(Entry) ->
    #{
        <<"type">> => <<"message">>,
        <<"topic">> => maps:get(topic, Entry, undefined),
        <<"data">> => maps:get(payload, Entry, #{}),
        <<"msgId">> => maps:get(message_id, Entry, undefined),
        <<"requiresAck">> => true,
        <<"isReplay">> => true
    }.

send(WsPid, Message) ->
    WsPid ! {send_to_client, Message}.
