%%%-------------------------------------------------------------------
%% @doc Subscription memory for reconnect restore.
%%
%% A client that reconnects with `reconnect: true` expects the server to
%% restore its subscriptions (the auth reply's `restoredSubscriptions`).
%% kraken used to restore only what the auth backend returned as
%% `active_subscriptions`, which a control plane had to persist from the
%% subscription reports. Neither built-in setup does that (static auth has
%% no such data, and nolag-core's example host does not store the reports),
%% so a reconnected client silently stopped receiving messages.
%%
%% This module remembers each connection's subscribe requests itself, so
%% kraken can replay them on reconnect without any external service.
%%
%% Key: {ActorTokenId, ClientId}. ClientId is the optional `clientId` from
%% the auth message (sanitised), or `undefined`. Without a clientId every
%% connection of one actor shares a key, so a reconnect restores the union
%% of that actor's subscriptions; give each client instance a clientId to
%% keep their sets apart.
%%
%% Entries live in a node-local ETS table. A reconnect may land on another
%% node, so recall/1 asks every connected node, and take/1 moves the
%% entries to the reconnecting node (the replayed subscribes are
%% remembered again there).
%%
%% Retention: entries are kept while any connection for the key is alive
%% (tracked with the kraken_resume syn scope) and for `resume_retention_ms`
%% (default one hour) after the last one closes.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_resume).
-behaviour(gen_server).

-export([start_link/0,
         key/2,
         register_connection/1,
         remember/3, update_filters/3, forget/2,
         take/1, recall/1, clear/1,
         fresh_connection/1, connection_closed/1]).
%% Called on remote nodes by recall/take/clear
-export([local_recall/1, local_clear/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TABLE, kraken_resume_subscriptions).
-define(SCOPE, kraken_resume).
-define(SWEEP_INTERVAL_MS, 60000).
-define(DEFAULT_RETENTION_MS, 3600000).
-define(RPC_TIMEOUT_MS, 2000).
%% Fields of a subscribe request worth replaying
-define(FIELDS, [<<"topic">>, <<"filters">>, <<"loadBalance">>, <<"loadBalanceGroup">>, <<"qos">>]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec key(binary(), term()) -> {binary(), binary() | undefined}.
key(ActorTokenId, ClientId) ->
    {ActorTokenId, kraken_session:sanitise_client_id(ClientId)}.

%% The calling connection process now holds Key. Keeps the key's entries
%% from expiring while it is alive (syn drops the membership when it dies).
register_connection(Key) ->
    catch syn:join(?SCOPE, {resume, Key}, self()),
    ok.

%% Remember a successful subscribe request (only the replayable fields).
remember(Key, Topic, SubscribeMessage) when is_binary(Topic), is_map(SubscribeMessage) ->
    Request = maps:with(?FIELDS, SubscribeMessage#{<<"topic">> => Topic}),
    ets:insert(?TABLE, {{Key, Topic}, Request, infinity}),
    ok;
remember(_Key, _Topic, _SubscribeMessage) ->
    ok.

%% setFilters changed the filters of a remembered subscription.
update_filters(Key, Topic, Filters) ->
    case ets:lookup(?TABLE, {Key, Topic}) of
        [{_, Request, Expiry}] ->
            Request1 = case Filters of
                [] -> maps:remove(<<"filters">>, Request);
                _ -> Request#{<<"filters">> => Filters}
            end,
            ets:insert(?TABLE, {{Key, Topic}, Request1, Expiry});
        [] ->
            ok
    end,
    ok.

forget(Key, Topic) ->
    ets:delete(?TABLE, {Key, Topic}),
    ok.

%% Subscribe requests remembered for Key on any node, newest wins per topic.
-spec recall(term()) -> [map()].
recall(Key) ->
    Remote = case nodes() of
        [] -> [];
        Nodes ->
            Results = erpc:multicall(Nodes, ?MODULE, local_recall, [Key], ?RPC_TIMEOUT_MS),
            lists:append([R || {ok, R} <- Results, is_list(R)])
    end,
    dedupe(local_recall(Key) ++ Remote).

%% recall/1, then drop the entries everywhere. The caller replays the
%% requests through the normal subscribe path, which remembers them again
%% on this node.
-spec take(term()) -> [map()].
take(Key) ->
    Requests = recall(Key),
    clear(Key),
    Requests.

%% Drop every entry for Key on every node.
clear(Key) ->
    local_clear(Key),
    case nodes() of
        [] -> ok;
        Nodes -> _ = erpc:multicall(Nodes, ?MODULE, local_clear, [Key], ?RPC_TIMEOUT_MS), ok
    end.

%% A connection authenticated WITHOUT asking for a restore. Whatever an
%% earlier, now-gone connection of this key subscribed to is stale: drop it,
%% unless another connection still holds the key.
fresh_connection(Key) ->
    case live_elsewhere(Key, self()) of
        true -> ok;
        false -> clear(Key)
    end.

%% A connection holding Key closed. Start the retention countdown unless
%% another connection still holds the key.
connection_closed(Key) ->
    Self = self(),
    catch syn:leave(?SCOPE, {resume, Key}, Self),
    case live_elsewhere(Key, Self) of
        true -> ok;
        false -> set_expiry(Key, erlang:monotonic_time(millisecond) + retention_ms())
    end,
    ok.

local_recall(Key) ->
    [Request || {_, Request, _} <- ets:match_object(?TABLE, {{Key, '_'}, '_', '_'})].

local_clear(Key) ->
    ets:match_delete(?TABLE, {{Key, '_'}, '_', '_'}),
    ok.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    ets:new(?TABLE, [named_table, public, set, {write_concurrency, true}, {read_concurrency, true}]),
    erlang:send_after(?SWEEP_INTERVAL_MS, self(), sweep),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, State) ->
    sweep(erlang:monotonic_time(millisecond)),
    erlang:send_after(?SWEEP_INTERVAL_MS, self(), sweep),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

retention_ms() ->
    case application:get_env(kraken, resume_retention_ms, ?DEFAULT_RETENTION_MS) of
        N when is_integer(N), N >= 0 -> N;
        _ -> ?DEFAULT_RETENTION_MS
    end.

set_expiry(Key, ExpiresAt) ->
    [ets:insert(?TABLE, {{Key, Topic}, Request, ExpiresAt})
     || {{_, Topic}, Request, _} <- ets:match_object(?TABLE, {{Key, '_'}, '_', '_'})],
    ok.

sweep(Now) ->
    Expired = ets:select(?TABLE, [{{'$1', '_', '$2'},
                                   [{'=/=', '$2', infinity}, {'<', '$2', Now}],
                                   ['$1']}]),
    lists:foreach(fun({Key, _Topic} = EntryKey) ->
        %% A connection may have re-registered the key since the countdown
        %% started; only drop entries nobody holds.
        case live_elsewhere(Key, undefined) of
            true -> ok;
            false -> ets:delete(?TABLE, EntryKey)
        end
    end, Expired).

live_elsewhere(Key, Except) ->
    case catch syn:members(?SCOPE, {resume, Key}) of
        Members when is_list(Members) ->
            lists:any(fun({Pid, _}) -> Pid =/= Except end, Members);
        _ ->
            false
    end.

dedupe(Requests) ->
    {_, Kept} = lists:foldl(fun(Request, {Seen, Acc}) ->
        Topic = maps:get(<<"topic">>, Request),
        case sets:is_element(Topic, Seen) of
            true -> {Seen, Acc};
            false -> {sets:add_element(Topic, Seen), [Request | Acc]}
        end
    end, {sets:new(), []}, Requests),
    lists:reverse(Kept).
