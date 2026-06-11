%%%-------------------------------------------------------------------
%% @doc Subscription Tracker
%% Reports subscription changes to Titus for persistence.
%% Batching gen_server: buffers track calls and flushes at BATCH_SIZE
%% items or every FLUSH_INTERVAL_MS, whichever comes first.
%% HTTP calls are spawned async so the gen_server never blocks on I/O.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_subscriptions).
-behaviour(gen_server).

-export([start_link/0, track/3, track/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(BATCH_SIZE, 50).
-define(FLUSH_INTERVAL_MS, 500).

-record(state, {
    buffer = [] :: list(),
    buffer_size = 0 :: non_neg_integer(),
    timer_ref = undefined :: undefined | reference()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Track a subscription change (async) - simple version
track(ActorTokenId, Topic, Action) ->
    track(ActorTokenId, Topic, Action, #{}).

%% Track a subscription change (async) with metadata
track(ActorTokenId, Topic, Action, Metadata) ->
    gen_server:cast(?SERVER, {track, ActorTokenId, Topic, Action, Metadata}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    TimerRef = schedule_flush(),
    {ok, #state{timer_ref = TimerRef}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({track, ActorTokenId, Topic, Action, Metadata}, State) ->
    Item = {ActorTokenId, Topic, Action, Metadata},
    NewSize = State#state.buffer_size + 1,
    NewState = State#state{buffer = [Item | State#state.buffer], buffer_size = NewSize},
    case NewSize >= ?BATCH_SIZE of
        true ->
            {noreply, do_flush(NewState)};
        false ->
            {noreply, NewState}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush, State) ->
    NewState = do_flush(State),
    TimerRef = schedule_flush(),
    {noreply, NewState#state{timer_ref = TimerRef}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

schedule_flush() ->
    erlang:send_after(?FLUSH_INTERVAL_MS, self(), flush).

do_flush(#state{buffer = [], buffer_size = 0} = State) ->
    State;
do_flush(#state{buffer = Buffer} = State) ->
    Items = lists:reverse(Buffer),
    spawn(fun() -> send_batch(Items) end),
    State#state{buffer = [], buffer_size = 0}.

send_batch(Items) ->
    Changes = lists:map(fun({ActorTokenId, Topic, Action, Metadata}) ->
        ActionBinary = case Action of
            subscribe -> <<"subscribe">>;
            unsubscribe -> <<"unsubscribe">>
        end,
        LoadBalance = maps:get(load_balance, Metadata, false),
        LoadBalanceGroup = maps:get(load_balance_group, Metadata, undefined),
        Filters = maps:get(filters, Metadata, undefined),
        Base = #{
            <<"actorTokenId">> => ActorTokenId,
            <<"topic">> => Topic,
            <<"action">> => ActionBinary
        },
        B1 = case LoadBalance of
            true ->
                B0 = maps:put(<<"loadBalance">>, true, Base),
                case LoadBalanceGroup of
                    undefined -> B0;
                    Group -> maps:put(<<"loadBalanceGroup">>, Group, B0)
                end;
            false -> Base
        end,
        case Filters of
            Fs when is_list(Fs), Fs =/= [] -> maps:put(<<"filters">>, Fs, B1);
            _ -> B1
        end
    end, Items),
    case kraken_control:report_subscription_change(Changes) of
        ok -> ok;
        {error, Reason} ->
            kraken_log:error("[SubTracker] Control-plane delivery failed: ~p", [Reason])
    end.

