%%%-------------------------------------------------------------------
%% @doc Replay Service
%% Handles message replay on actor reconnect with server-side buffering
%% to prevent race conditions between replay query and live messages.
%%
%% Flow:
%% 1. Actor connects
%% 2. Create buffer for live messages
%% 3. Subscribe to topics (messages go to buffer)
%% 4. Query undelivered messages from Titus
%% 5. Send REPLAY_START → replayed messages → REPLAY_END
%% 6. Flush buffer (deduplicated against replayed)
%% 7. Switch to live delivery
%% @end
%%%-------------------------------------------------------------------
-module(kraken_replay).

%% API
-export([
    start_replay/4,
    buffer_message/2,
    is_replaying/1,
    get_replayed_message_ids/1,
    cleanup_replay_state/1,
    handle_replay_complete/3
]).

-define(REPLAY_STATE_TABLE, replay_state).

-record(replay_state, {
    actor_id :: binary(),
    app_id :: binary(),
    buffer :: queue:queue(),  % Messages received during replay
    replayed_ids :: sets:set(), % Message IDs already replayed (for dedup)
    status :: buffering | replaying | flushing | done,
    ws_pid :: pid()
}).

%%====================================================================
%% API
%%====================================================================

%% Initialize replay state for an actor
%% Called when actor connects and needs replay
-spec start_replay(
    ActorId :: binary(),
    AppId :: binary(),
    Topics :: [binary()] | undefined,
    WsPid :: pid()
) -> {ok, started} | {error, term()}.
start_replay(ActorId, AppId, Topics, WsPid) ->
    %% Initialize replay state
    State = #replay_state{
        actor_id = ActorId,
        app_id = AppId,
        buffer = queue:new(),
        replayed_ids = sets:new(),
        status = buffering,
        ws_pid = WsPid
    },

    %% Store in process dictionary (per-connection)
    put({replay_state, ActorId}, State),

    %% Start async replay process
    Self = self(),
    spawn(fun() ->
        do_replay(ActorId, AppId, Topics, WsPid, Self)
    end),

    {ok, started}.

%% Buffer a live message during replay
%% Returns true if message was buffered, false if not replaying
-spec buffer_message(ActorId :: binary(), Message :: map()) -> boolean().
buffer_message(ActorId, Message) ->
    case get({replay_state, ActorId}) of
        #replay_state{status = Status, buffer = Buffer} = State when Status =:= buffering; Status =:= replaying ->
            %% Add message to buffer
            NewBuffer = queue:in(Message, Buffer),
            put({replay_state, ActorId}, State#replay_state{buffer = NewBuffer}),
            true;
        _ ->
            %% Not replaying, deliver normally
            false
    end.

%% Check if actor is currently in replay mode
-spec is_replaying(ActorId :: binary()) -> boolean().
is_replaying(ActorId) ->
    case get({replay_state, ActorId}) of
        #replay_state{status = Status} when Status =/= done ->
            true;
        _ ->
            false
    end.

%% Get set of replayed message IDs (for external dedup if needed)
-spec get_replayed_message_ids(ActorId :: binary()) -> sets:set().
get_replayed_message_ids(ActorId) ->
    case get({replay_state, ActorId}) of
        #replay_state{replayed_ids = Ids} ->
            Ids;
        _ ->
            sets:new()
    end.

%% Cleanup replay state after completion
-spec cleanup_replay_state(ActorId :: binary()) -> ok.
cleanup_replay_state(ActorId) ->
    erase({replay_state, ActorId}),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% Perform the actual replay
do_replay(ActorId, AppId, Topics, WsPid, ParentPid) ->
    %% Fetch undelivered messages from Titus
    MaxMessages = 100, % TODO: Get from topic config
    case kraken_store:get_replay_messages(#{
        actor_id => ActorId, app_id => AppId,
        topics => Topics, limit => MaxMessages
    }) of
        {ok, Messages, Count} ->
            %% Update state to replaying
            update_replay_status(ParentPid, ActorId, replaying),

            %% Collect replayed message IDs for deduplication
            ReplayedIds = lists:foldl(fun(Msg, Acc) ->
                MsgId = maps:get(<<"messageId">>, Msg, undefined),
                case MsgId of
                    undefined -> Acc;
                    _ -> sets:add_element(MsgId, Acc)
                end
            end, sets:new(), Messages),

            %% Store replayed IDs for dedup
            update_replayed_ids(ParentPid, ActorId, ReplayedIds),

            %% Send REPLAY_START
            send_to_ws(WsPid, #{
                type => <<"replayStart">>,
                count => Count,
                oldestTimestamp => get_oldest_timestamp(Messages),
                newestTimestamp => get_newest_timestamp(Messages)
            }),

            %% Send replayed messages (with isReplay flag)
            lists:foreach(fun(Msg) ->
                send_to_ws(WsPid, Msg#{<<"isReplay">> => true})
            end, Messages),

            %% Send REPLAY_END
            send_to_ws(WsPid, #{
                type => <<"replayEnd">>,
                replayed => Count
            }),

            %% Signal parent to flush buffer
            ParentPid ! {replay_complete, ActorId, ReplayedIds};

        {error, Reason} ->
            kraken_log:error("[ReplayService] Error fetching replay messages: ~p", [Reason]),
            %% Send empty replay
            send_to_ws(WsPid, #{type => <<"replayStart">>, count => 0}),
            send_to_ws(WsPid, #{type => <<"replayEnd">>, replayed => 0}),
            ParentPid ! {replay_complete, ActorId, sets:new()}
    end.

%% Update replay status in parent process
update_replay_status(ParentPid, ActorId, Status) ->
    ParentPid ! {update_replay_status, ActorId, Status}.

%% Update replayed IDs in parent process
update_replayed_ids(ParentPid, ActorId, ReplayedIds) ->
    ParentPid ! {update_replayed_ids, ActorId, ReplayedIds}.

%% Send message to WebSocket process
send_to_ws(WsPid, Message) ->
    WsPid ! {send_to_client, Message}.

%% Get oldest timestamp from messages
get_oldest_timestamp([]) ->
    null;
get_oldest_timestamp([First | _]) ->
    maps:get(<<"timestamp">>, First, null).

%% Get newest timestamp from messages
get_newest_timestamp([]) ->
    null;
get_newest_timestamp(Messages) ->
    Last = lists:last(Messages),
    maps:get(<<"timestamp">>, Last, null).

%%====================================================================
%% Message handlers for kraken_ws_handler to call
%%====================================================================

%% Handle replay_complete message in kraken_ws_handler
%% Call this from kraken_ws_handler when receiving {replay_complete, ActorId, ReplayedIds}
-spec handle_replay_complete(ActorId :: binary(), ReplayedIds :: sets:set(), WsPid :: pid()) -> ok.
handle_replay_complete(ActorId, ReplayedIds, WsPid) ->
    case get({replay_state, ActorId}) of
        #replay_state{buffer = Buffer} = State ->
            %% Update status to flushing
            put({replay_state, ActorId}, State#replay_state{
                status = flushing,
                replayed_ids = ReplayedIds
            }),

            %% Flush buffer (deduplicated)
            BufferedMessages = queue:to_list(Buffer),
            NewMessages = lists:filter(fun(Msg) ->
                MsgId = maps:get(<<"msgId">>, Msg, maps:get(<<"messageId">>, Msg, undefined)),
                not sets:is_element(MsgId, ReplayedIds)
            end, BufferedMessages),

            %% Send buffered messages (not replay)
            lists:foreach(fun(Msg) ->
                WsPid ! {send_to_client, Msg#{<<"isReplay">> => false}}
            end, NewMessages),

            %% Mark as done and cleanup
            put({replay_state, ActorId}, State#replay_state{status = done}),
            cleanup_replay_state(ActorId),
            ok;
        _ ->
            ok
    end.
