%%%-------------------------------------------------------------------
%% @doc Long-lived broker sessions for publishers that have no connection of
%% their own (the HTTP publish route).
%%
%% Each WebSocket owns a broker client; an HTTP request lives for one call,
%% and connecting to the broker per request — what /internal/publish does —
%% costs a TCP + CONNECT round trip every time. This process keeps a few
%% sessions open and hands them out. Connecting never crashes it: a broker
%% that isn't up yet is retried, and callers get {error, unavailable} until
%% one session is ready.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_publisher_pool).
-behaviour(gen_server).

-export([start_link/0, child_spec/0, session/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TABLE, kraken_publisher_sessions).
-define(RECONNECT_MS, 2000).

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        type => worker,
        modules => [?MODULE]
    }.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% A session to publish on, spread across the pool.
-spec session() -> {ok, term()} | {error, unavailable}.
session() ->
    try ets:tab2list(?TABLE) of
        [] -> {error, unavailable};
        Sessions ->
            {_Index, Session} = lists:nth(rand:uniform(length(Sessions)), Sessions),
            {ok, Session}
    catch
        error:badarg -> {error, unavailable}
    end.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),
    ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    Size = pool_size(),
    [self() ! {connect, I} || I <- lists:seq(1, Size)],
    {ok, #{owners => #{}}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({connect, I}, #{owners := Owners} = State) ->
    case catch kraken_broker:connect() of
        {ok, Session} ->
            ets:insert(?TABLE, {I, Session}),
            Owners1 = case Session of
                Pid when is_pid(Pid) -> Owners#{Pid => I};
                _ -> Owners
            end,
            {noreply, State#{owners := Owners1}};
        Other ->
            kraken_log:error("[PublisherPool] Session ~p could not connect: ~p", [I, Other]),
            erlang:send_after(?RECONNECT_MS, self(), {connect, I}),
            {noreply, State}
    end;
%% A broker client (linked to us) died: drop it and connect a replacement.
handle_info({'EXIT', Pid, Reason}, #{owners := Owners} = State) ->
    case maps:take(Pid, Owners) of
        {I, Owners1} ->
            kraken_log:info("[PublisherPool] Session ~p lost (~p), reconnecting", [I, Reason]),
            ets:delete(?TABLE, I),
            erlang:send_after(?RECONNECT_MS, self(), {connect, I}),
            {noreply, State#{owners := Owners1}};
        error ->
            {noreply, State}
    end;
%% The mqtt backend's message handler reports to the connecting process;
%% publishers never subscribe, so there is nothing to deliver.
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    [catch kraken_broker:disconnect(S) || {_, S} <- ets:tab2list(?TABLE)],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

pool_size() ->
    case application:get_env(kraken, publisher_pool_size) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> 2
    end.
