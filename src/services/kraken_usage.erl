%%%-------------------------------------------------------------------
%% @doc Usage Notifier Service
%% Batches message counts and bytes per project and sends to Titus for billing.
%% Only notifies Titus of activity - doesn't poll inactive projects.
%%
%% Sends batched counts every FLUSH_INTERVAL_MS or when a project
%% reaches FLUSH_THRESHOLD messages.
%%
%% Titus responds with `limitExceeded` project IDs when an organization
%% exceeds its monthly message quota. These are cached in an ETS table
%% and checked by kraken_store before logging messages.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_usage).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    increment/2,
    increment/3,
    flush/0,
    get_stats/0,
    is_project_blocked/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(FLUSH_INTERVAL_MS, 30000).  %% Flush every 30 seconds
-define(FLUSH_THRESHOLD, 100).      %% Flush a project after 100 messages
-define(BLOCKED_TABLE, usage_blocked_projects).

-record(state, {
    counts = #{} :: #{binary() => {non_neg_integer(), non_neg_integer()}},  %% ProjectId => {Count, Bytes}
    total_sent = 0 :: non_neg_integer()  %% For stats
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Increment message count for a project (async, no bytes)
-spec increment(ProjectId :: binary(), Count :: non_neg_integer()) -> ok.
increment(ProjectId, Count) when is_binary(ProjectId), Count > 0 ->
    gen_server:cast(?SERVER, {increment, ProjectId, Count, 0});
increment(_, _) ->
    ok.

%% Increment message count and bytes for a project (async)
-spec increment(ProjectId :: binary(), Count :: non_neg_integer(), Bytes :: non_neg_integer()) -> ok.
increment(ProjectId, Count, Bytes) when is_binary(ProjectId), Count > 0 ->
    gen_server:cast(?SERVER, {increment, ProjectId, Count, Bytes});
increment(_, _, _) ->
    ok.

%% Force flush all counts (sync)
-spec flush() -> ok.
flush() ->
    gen_server:call(?SERVER, flush, 10000).

%% Get current stats (for debugging)
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%% Check if a project has exceeded its monthly message limit.
%% Called by kraken_store before logging messages.
-spec is_project_blocked(ProjectId :: binary()) -> boolean().
is_project_blocked(ProjectId) when is_binary(ProjectId) ->
    try
        case ets:lookup(?BLOCKED_TABLE, ProjectId) of
            [{_, true}] -> true;
            _ -> false
        end
    catch
        error:badarg -> false  %% Table not yet created
    end;
is_project_blocked(_) ->
    false.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% ETS table for blocked projects (set, public for fast reads from other processes)
    ets:new(?BLOCKED_TABLE, [named_table, set, public, {read_concurrency, true}]),
    kraken_log:info("[UsageNotifier] Started (delivering via kraken_control)", []),
    kraken_log:info("[UsageNotifier] Flush interval: ~pms, threshold: ~p messages",
              [?FLUSH_INTERVAL_MS, ?FLUSH_THRESHOLD]),
    %% Schedule periodic flush
    erlang:send_after(?FLUSH_INTERVAL_MS, self(), flush_timer),
    {ok, #state{}}.

handle_call(flush, _From, State) ->
    NewState = do_flush_all(State),
    {reply, ok, NewState};

handle_call(get_stats, _From, #state{counts = Counts, total_sent = TotalSent} = State) ->
    Stats = #{
        pending_counts => Counts,
        pending_projects => maps:size(Counts),
        pending_total => lists:sum([C || {C, _} <- maps:values(Counts)]),
        total_sent => TotalSent,
        blocked_projects => ets:info(?BLOCKED_TABLE, size)
    },
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({increment, ProjectId, Count, Bytes}, #state{counts = Counts} = State) ->
    {CurrentCount, CurrentBytes} = maps:get(ProjectId, Counts, {0, 0}),
    NewCount = CurrentCount + Count,
    NewBytes = CurrentBytes + Bytes,
    NewCounts = maps:put(ProjectId, {NewCount, NewBytes}, Counts),

    %% Check if this project reached the threshold
    NewState = case NewCount >= ?FLUSH_THRESHOLD of
        true ->
            %% Flush just this project
            do_flush_project(ProjectId, NewCount, NewBytes, State#state{counts = maps:remove(ProjectId, NewCounts)});
        false ->
            State#state{counts = NewCounts}
    end,
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush_timer, State) ->
    NewState = do_flush_all(State),
    erlang:send_after(?FLUSH_INTERVAL_MS, self(), flush_timer),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Flush remaining on shutdown
    do_flush_all(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

do_flush_all(#state{counts = Counts} = State) when map_size(Counts) =:= 0 ->
    %% Even with no pending counts, refresh the blocked projects list
    %% to unblock projects that upgraded their plan
    refresh_blocked_projects(State);
do_flush_all(#state{counts = Counts, total_sent = TotalSent} = State) ->
    %% Build batch request with all project counts
    UsageEntries = maps:fold(fun(ProjectId, {Count, Bytes}, Acc) ->
        [#{<<"projectId">> => ProjectId, <<"count">> => Count, <<"totalBytes">> => Bytes} | Acc]
    end, [], Counts),

    TotalCount = lists:sum([C || {C, _} <- maps:values(Counts)]),
    kraken_log:info("[UsageNotifier] Flushing ~p projects, ~p total messages",
              [maps:size(Counts), TotalCount]),

    case send_usage_batch(UsageEntries) of
        ok ->
            State#state{counts = #{}, total_sent = TotalSent + TotalCount};
        {error, _Reason} ->
            %% Keep counts on error - will retry on next flush
            State
    end.

do_flush_project(ProjectId, Count, Bytes, #state{total_sent = TotalSent} = State) ->
    kraken_log:info("[UsageNotifier] Flushing project ~s: ~p messages (threshold reached)",
              [ProjectId, Count]),

    UsageEntries = [#{<<"projectId">> => ProjectId, <<"count">> => Count, <<"totalBytes">> => Bytes}],

    case send_usage_batch(UsageEntries) of
        ok ->
            State#state{total_sent = TotalSent + Count};
        {error, _Reason} ->
            %% Put count back on error
            Counts = State#state.counts,
            {ExistingCount, ExistingBytes} = maps:get(ProjectId, Counts, {0, 0}),
            State#state{counts = maps:put(ProjectId, {ExistingCount + Count, ExistingBytes + Bytes}, Counts)}
    end.

send_usage_batch(UsageEntries) ->
    case kraken_control:report_usage(UsageEntries) of
        ok ->
            ok;
        {ok, BlockedProjects} when is_list(BlockedProjects) ->
            %% Replace the blocked set wholesale so projects that
            %% upgraded their plan get unblocked.
            ets:delete_all_objects(?BLOCKED_TABLE),
            lists:foreach(fun(ProjectId) ->
                ets:insert(?BLOCKED_TABLE, {ProjectId, true}),
                kraken_log:info("[UsageNotifier] Project ~s exceeded quota (control plane)", [ProjectId])
            end, BlockedProjects),
            ok;
        {error, Reason} ->
            kraken_log:error("[UsageNotifier] Control-plane usage delivery failed: ~p", [Reason]),
            {error, Reason}
    end.

%% Refresh the blocked projects list even when no counts are pending.
%% Sends an empty usage batch to Titus so it returns an up-to-date limitExceeded list.
%% This unblocks projects that upgraded their plan while blocked.
refresh_blocked_projects(State) ->
    case ets:info(?BLOCKED_TABLE, size) of
        0 -> State;  %% No blocked projects, nothing to refresh
        _ ->
            case send_usage_batch([]) of
                ok -> State;
                {error, _} -> State
            end
    end.

