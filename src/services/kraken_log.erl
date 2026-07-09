%%%-------------------------------------------------------------------
%% @doc Async Logger
%% Non-blocking logger that offloads io:format to a dedicated process.
%% Callers use gen_server:cast (fire-and-forget), so the message
%% pipeline is never blocked by stdout I/O.
%%
%% Overload protection: io:format is synchronous, so a stalled stdout (a
%% slow log consumer capturing the container's output) blocks this single
%% process inside io:format while every caller keeps casting — the mailbox
%% would grow until the node OOMs. The guard is caller-side (checking the
%% logger's mailbox depth before enqueuing): once the process is blocked in
%% io:format it can't self-check, so a handle_cast-side drop wouldn't help.
%% Past the high-water mark we drop log casts, so logging sheds load instead
%% of taking the node down.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_log).
-behaviour(gen_server).

-export([start_link/0, info/2, error/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(MAILBOX_HIGH_WATER, 10000).  %% drop logs once the backlog exceeds this

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec info(string(), list()) -> ok.
info(Fmt, Args) ->
    maybe_log(Fmt, Args).

-spec error(string(), list()) -> ok.
error(Fmt, Args) ->
    maybe_log(Fmt, Args).

%% Enqueue unless the logger is down (tests) or its mailbox is backed up
%% past the high-water mark (stalled/slow stdout). message_queue_len is an
%% O(1) counter read.
maybe_log(Fmt, Args) ->
    case whereis(?MODULE) of
        undefined ->
            ok;
        Pid ->
            case erlang:process_info(Pid, message_queue_len) of
                {message_queue_len, N} when N > ?MAILBOX_HIGH_WATER ->
                    ok;
                _ ->
                    gen_server:cast(?MODULE, {log, Fmt, Args})
            end
    end.

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
