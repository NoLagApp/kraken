%%%-------------------------------------------------------------------
%% @doc Gun HTTP Client
%% Manages a persistent gun connection to a target host.
%% Requests go directly from the caller's process (no serialization).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_gun_client).
-behaviour(gen_server).

-export([start_link/2, start_link/3, request/5, request/6]).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, terminate/2]).

-define(ETS_TABLE, kraken_gun_client_conns).
-define(DEFAULT_TIMEOUT, 5000).
-define(POOL_COUNTER, kraken_gun_client_pool_counter).

%%====================================================================
%% API
%%====================================================================

start_link(Name, Url) ->
    gen_server:start_link({local, kraken_gun_client_name(Name)}, ?MODULE, [Name, Url], []).

%% Start a pooled connection: {Name, Index}
start_link(Name, Url, Index) ->
    PoolName = {Name, Index},
    gen_server:start_link({local, kraken_gun_client_name(PoolName)}, ?MODULE, [PoolName, Url], []).

request(Name, Method, Path, Headers, Body) ->
    request(Name, Method, Path, Headers, Body, ?DEFAULT_TIMEOUT).

request(Name, Method, Path, Headers, Body, Timeout) ->
    LookupName = pick_connection(Name),
    case ets:lookup(?ETS_TABLE, LookupName) of
        [{LookupName, ConnPid}] ->
            do_request(LookupName, ConnPid, Method, Path, Headers, Body, Timeout);
        [] ->
            {error, no_connection}
    end.

%% Round-robin across pool members, or use single connection
pick_connection(Name) ->
    case ets:lookup(?ETS_TABLE, {pool_size, Name}) of
        [{{pool_size, Name}, Size}] ->
            Index = ets:update_counter(?POOL_COUNTER, Name, {2, 1, Size, 1}, {Name, 0}),
            {Name, Index};
        [] ->
            Name
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Name, Url]) ->
    case ets:info(?ETS_TABLE, size) of
        undefined ->
            ets:new(?ETS_TABLE, [named_table, public, set, {read_concurrency, true}]),
            ets:new(?POOL_COUNTER, [named_table, public, set, {write_concurrency, true}]);
        _ -> ok
    end,
    %% Register pool size for pooled connections
    case Name of
        {BaseName, _Index} ->
            case ets:lookup(?ETS_TABLE, {pool_size, BaseName}) of
                [{{pool_size, BaseName}, OldSize}] ->
                    ets:insert(?ETS_TABLE, {{pool_size, BaseName}, max(OldSize, element(2, Name))});
                [] ->
                    ets:insert(?ETS_TABLE, {{pool_size, BaseName}, element(2, Name)})
            end;
        _ -> ok
    end,
    UrlBin = if is_list(Url) -> list_to_binary(Url); true -> Url end,
    #{host := Host} = Parsed = uri_string:parse(UrlBin),
    Scheme = maps:get(scheme, Parsed, <<"https">>),
    Port = maps:get(port, Parsed, default_port(Scheme)),
    State = #{
        name => Name,
        host => binary_to_list(Host),
        port => Port,
        scheme => Scheme,
        conn_pid => undefined,
        mon_ref => undefined
    },
    %% Connect asynchronously so GCP connections don't block the supervisor
    self() ! connect,
    {ok, State}.

handle_info(connect, State) ->
    {noreply, try_connect(State)};
handle_info({gun_down, ConnPid, _Protocol, _Reason, _Streams}, #{conn_pid := ConnPid} = State) ->
    kraken_log:info("[kraken_gun_client] Connection down, gun will reconnect", []),
    {noreply, State};
handle_info({gun_up, ConnPid, _Protocol}, #{conn_pid := ConnPid, name := Name} = State) ->
    kraken_log:info("[kraken_gun_client] Connection back up for ~p", [Name]),
    {noreply, State};
handle_info({'DOWN', MonRef, process, ConnPid, Reason}, #{mon_ref := MonRef, conn_pid := ConnPid} = State) ->
    kraken_log:info("[kraken_gun_client] Gun process died: ~p, reconnecting", [Reason]),
    ets:delete(?ETS_TABLE, maps:get(name, State)),
    erlang:send_after(500, self(), connect),
    {noreply, State#{conn_pid := undefined, mon_ref := undefined}};
handle_info(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #{conn_pid := ConnPid}) when ConnPid =/= undefined ->
    gun:close(ConnPid),
    ok;
terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

try_connect(#{host := Host, port := Port, scheme := Scheme, name := Name} = State) ->
    GunOpts0 = #{
        retry => 5,
        retry_timeout => 1000
    },
    GunOpts = case Scheme of
        <<"https">> ->
            GunOpts0#{
                transport => tls,
                tls_opts => [{verify, verify_none}],
                protocols => [http2, http]
            };
        _ ->
            GunOpts0#{
                protocols => [http]
            }
    end,
    case gun:open(Host, Port, GunOpts) of
        {ok, ConnPid} ->
            MonRef = monitor(process, ConnPid),
            case gun:await_up(ConnPid, 5000) of
                {ok, _Protocol} ->
                    ets:insert(?ETS_TABLE, {Name, ConnPid}),
                    State#{conn_pid := ConnPid, mon_ref := MonRef};
                {error, Reason} ->
                    kraken_log:info("[kraken_gun_client] Failed to connect to ~s:~p (~p): ~p, will retry", [Host, Port, Name, Reason]),
                    gun:close(ConnPid),
                    demonitor(MonRef, [flush]),
                    erlang:send_after(2000, self(), connect),
                    State
            end;
        {error, Reason} ->
            kraken_log:info("[kraken_gun_client] Failed to open connection to ~s:~p (~p): ~p, will retry", [Host, Port, Name, Reason]),
            erlang:send_after(2000, self(), connect),
            State
    end.

do_request(Name, ConnPid, Method, Path, Headers, Body, Timeout) ->
    try
        StreamRef = case Method of
            get -> gun:get(ConnPid, Path, Headers);
            post -> gun:post(ConnPid, Path, Headers, Body);
            put -> gun:put(ConnPid, Path, Headers, Body);
            patch -> gun:patch(ConnPid, Path, Headers, Body);
            delete -> gun:delete(ConnPid, Path, Headers)
        end,
        case gun:await(ConnPid, StreamRef, Timeout) of
            {response, fin, Status, _RespHeaders} ->
                {ok, Status, <<>>};
            {response, nofin, Status, _RespHeaders} ->
                case gun:await_body(ConnPid, StreamRef, Timeout) of
                    {ok, RespBody} ->
                        {ok, Status, RespBody};
                    {error, BodyReason} ->
                        {error, BodyReason}
                end;
            {error, Reason} ->
                {error, Reason}
        end
    catch
        exit:{normal, _} ->
            retry_request(Name, Method, Path, Headers, Body, Timeout);
        exit:{noproc, _} ->
            retry_request(Name, Method, Path, Headers, Body, Timeout)
    end.

retry_request(Name, Method, Path, Headers, Body, Timeout) ->
    timer:sleep(100),
    case ets:lookup(?ETS_TABLE, Name) of
        [{Name, NewConnPid}] ->
            try
                StreamRef = case Method of
                    get -> gun:get(NewConnPid, Path, Headers);
                    post -> gun:post(NewConnPid, Path, Headers, Body);
                    put -> gun:put(NewConnPid, Path, Headers, Body);
                    patch -> gun:patch(NewConnPid, Path, Headers, Body);
                    delete -> gun:delete(NewConnPid, Path, Headers)
                end,
                case gun:await(NewConnPid, StreamRef, Timeout) of
                    {response, fin, Status, _} ->
                        {ok, Status, <<>>};
                    {response, nofin, Status, _} ->
                        case gun:await_body(NewConnPid, StreamRef, Timeout) of
                            {ok, RespBody} -> {ok, Status, RespBody};
                            {error, R} -> {error, R}
                        end;
                    {error, R} ->
                        {error, R}
                end
            catch
                _:_ -> {error, connection_lost}
            end;
        [] ->
            {error, no_connection}
    end.

kraken_gun_client_name({Name, Index}) ->
    list_to_atom("kraken_gun_client_" ++ atom_to_list(Name) ++ "_" ++ integer_to_list(Index));
kraken_gun_client_name(Name) ->
    list_to_atom("kraken_gun_client_" ++ atom_to_list(Name)).

default_port(<<"https">>) -> 443;
default_port(<<"http">>) -> 80;
default_port(_) -> 443.
