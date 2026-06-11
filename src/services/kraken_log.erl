%%%-------------------------------------------------------------------
%% @doc Async Logger
%% Non-blocking logger that offloads io:format to a dedicated process.
%% Callers use gen_server:cast (fire-and-forget), so the message
%% pipeline is never blocked by stdout I/O.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_log).
-behaviour(gen_server).

-export([start_link/0, info/2, error/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec info(string(), list()) -> ok.
info(Fmt, Args) ->
    gen_server:cast(?MODULE, {log, Fmt, Args}).

-spec error(string(), list()) -> ok.
error(Fmt, Args) ->
    gen_server:cast(?MODULE, {log, Fmt, Args}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({log, Fmt, Args}, State) ->
    io:format(Fmt, Args),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
