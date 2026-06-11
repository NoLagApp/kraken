%%%-------------------------------------------------------------------
%% @doc Cluster Manager
%% Handles cluster formation and node discovery using native Erlang.
%% Supports multiple discovery strategies:
%%   - standalone: No clustering (single node)
%%   - dns: DNS-based discovery (Kubernetes headless services)
%%   - epmd: EPMD-based discovery with static hosts
%%   - gossip: UDP multicast discovery (same subnet)
%%
%% @end
%%%-------------------------------------------------------------------
-module(kraken_cluster).
-behaviour(gen_server).

%% API
-export([start_link/0, get_nodes/0, get_strategy/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    strategy :: atom(),
    poll_interval :: integer(),
    poll_timer :: reference() | undefined,
    gossip_socket :: port() | undefined,
    gossip_multicast_addr :: tuple() | undefined,
    gossip_port :: integer()
}).

-define(DEFAULT_POLL_INTERVAL, 30000).
-define(DEFAULT_GOSSIP_PORT, 45892).
-define(DEFAULT_MULTICAST_ADDR, {230, 1, 1, 1}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_nodes() ->
    [node() | nodes()].

get_strategy() ->
    gen_server:call(?MODULE, get_strategy).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    net_kernel:monitor_nodes(true, [{node_type, visible}]),

    Strategy = get_cluster_strategy(),
    PollInterval = get_poll_interval(),

    kraken_log:info("[ClusterManager] Starting with strategy: ~p~n", [Strategy]),
    kraken_log:info("[ClusterManager] Node name: ~p~n", [node()]),
    kraken_log:info("[ClusterManager] Cookie: ~p~n", [erlang:get_cookie()]),

    State0 = #state{
        strategy = Strategy,
        poll_interval = PollInterval
    },

    State = case Strategy of
        standalone ->
            kraken_log:info("[ClusterManager] Running in standalone mode (no clustering)~n", []),
            State0;
        dns ->
            kraken_log:info("[ClusterManager] Using DNS discovery~n", []),
            Timer = erlang:send_after(0, self(), poll_dns),
            State0#state{poll_timer = Timer};
        epmd ->
            kraken_log:info("[ClusterManager] Using EPMD discovery~n", []),
            Timer = erlang:send_after(0, self(), poll_epmd),
            State0#state{poll_timer = Timer};
        gossip ->
            kraken_log:info("[ClusterManager] Using Gossip/Multicast discovery~n", []),
            {ok, Socket, MulticastAddr, Port} = setup_gossip(),
            Timer = erlang:send_after(0, self(), gossip_announce),
            State0#state{
                poll_timer = Timer,
                gossip_socket = Socket,
                gossip_multicast_addr = MulticastAddr,
                gossip_port = Port
            }
    end,

    {ok, State}.

handle_call(get_strategy, _From, State) ->
    {reply, State#state.strategy, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll_dns, #state{poll_interval = Interval} = State) ->
    discover_via_dns(),
    Timer = erlang:send_after(Interval, self(), poll_dns),
    {noreply, State#state{poll_timer = Timer}};

handle_info(poll_epmd, #state{poll_interval = Interval} = State) ->
    discover_via_epmd(),
    Timer = erlang:send_after(Interval, self(), poll_epmd),
    {noreply, State#state{poll_timer = Timer}};

handle_info({nodeup, Node, _Info}, State) ->
    kraken_log:info("[ClusterManager] Node joined: ~p~n", [Node]),
    kraken_log:info("[ClusterManager] Current cluster: ~p~n", [get_nodes()]),
    {noreply, State};

handle_info({nodedown, Node, _Info}, State) ->
    kraken_log:info("[ClusterManager] Node left: ~p~n", [Node]),
    kraken_log:info("[ClusterManager] Current cluster: ~p~n", [get_nodes()]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{gossip_socket = Socket}) ->
    net_kernel:monitor_nodes(false),
    case Socket of
        undefined -> ok;
        _ -> gen_udp:close(Socket)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions - Configuration
%%====================================================================

get_cluster_strategy() ->
    case os:getenv("CLUSTER_STRATEGY") of
        false -> standalone;
        "standalone" -> standalone;
        "dns" -> dns;
        "gossip" -> gossip;
        "epmd" -> epmd;
        Other ->
            kraken_log:info("[ClusterManager] Unknown strategy '~s', using standalone~n", [Other]),
            standalone
    end.

get_poll_interval() ->
    case os:getenv("CLUSTER_POLL_INTERVAL") of
        false -> ?DEFAULT_POLL_INTERVAL;
        Val -> list_to_integer(Val)
    end.

%%====================================================================
%% Internal functions - DNS Discovery
%%====================================================================

discover_via_dns() ->
    Query = os:getenv("CLUSTER_DNS_QUERY", ""),
    NodeBasename = os:getenv("CLUSTER_NODE_BASENAME", "kraken_proxy"),

    case Query of
        "" ->
            kraken_log:info("[ClusterManager] No DNS query configured~n", []);
        _ ->
            case inet_res:lookup(Query, in, a) of
                [] ->
                    kraken_log:info("[ClusterManager] DNS query returned no results: ~s~n", [Query]);
                IPs ->
                    kraken_log:info("[ClusterManager] DNS discovered IPs: ~p~n", [IPs]),
                    lists:foreach(fun(IP) ->
                        NodeName = list_to_atom(NodeBasename ++ "@" ++ inet:ntoa(IP)),
                        connect_if_not_self(NodeName)
                    end, IPs)
            end
    end.

%%====================================================================
%% Internal functions - EPMD Discovery
%%====================================================================

discover_via_epmd() ->
    HostsStr = os:getenv("CLUSTER_HOSTS", ""),
    case HostsStr of
        "" ->
            kraken_log:info("[ClusterManager] No CLUSTER_HOSTS configured~n", []);
        _ ->
            Hosts = string:tokens(HostsStr, ","),
            lists:foreach(fun(HostStr) ->
                NodeName = list_to_atom(string:trim(HostStr)),
                connect_if_not_self(NodeName)
            end, Hosts)
    end.

%%====================================================================
%% Internal functions - Gossip Discovery
%%====================================================================

setup_gossip() ->
    Port = case os:getenv("CLUSTER_GOSSIP_PORT") of
        false -> ?DEFAULT_GOSSIP_PORT;
        P -> list_to_integer(P)
    end,

    MulticastAddr = case os:getenv("CLUSTER_MULTICAST_ADDR") of
        false -> ?DEFAULT_MULTICAST_ADDR;
        Addr ->
            {ok, IP} = inet:parse_address(Addr),
            IP
    end,

    {ok, Socket} = gen_udp:open(Port, [
        binary,
        {active, true},
        {reuseaddr, true},
        {multicast_ttl, 1},
        {multicast_loop, true},
        {add_membership, {MulticastAddr, {0, 0, 0, 0}}}
    ]),

    kraken_log:info("[ClusterManager] Gossip socket opened on port ~p, multicast ~p~n",
              [Port, MulticastAddr]),

    {ok, Socket, MulticastAddr, Port}.

send_gossip_announce(Socket, MulticastAddr, Port) ->
    NodeBin = atom_to_binary(node(), utf8),
    Cookie = erlang:get_cookie(),
    CookieHash = crypto:hash(sha256, atom_to_binary(Cookie, utf8)),
    Message = <<CookieHash/binary, NodeBin/binary>>,
    gen_udp:send(Socket, MulticastAddr, Port, Message).

handle_gossip_message(<<CookieHash:32/binary, NodeBin/binary>>) ->
    OurCookie = erlang:get_cookie(),
    OurHash = crypto:hash(sha256, atom_to_binary(OurCookie, utf8)),
    case CookieHash of
        OurHash ->
            NodeName = binary_to_atom(NodeBin, utf8),
            connect_if_not_self(NodeName);
        _ ->
            ok
    end;
handle_gossip_message(_) ->
    ok.

%%====================================================================
%% Internal functions - Connection
%%====================================================================

connect_if_not_self(NodeName) ->
    case NodeName of
        N when N =:= node() ->
            ok;
        _ ->
            case lists:member(NodeName, nodes()) of
                true ->
                    ok;
                false ->
                    kraken_log:info("[ClusterManager] Attempting to connect to: ~p~n", [NodeName]),
                    case net_kernel:connect_node(NodeName) of
                        true ->
                            kraken_log:info("[ClusterManager] Successfully connected to: ~p~n", [NodeName]);
                        false ->
                            kraken_log:info("[ClusterManager] Failed to connect to: ~p~n", [NodeName]);
                        ignored ->
                            kraken_log:info("[ClusterManager] Connection ignored for: ~p~n", [NodeName])
                    end
            end
    end.
