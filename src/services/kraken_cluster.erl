%%%-------------------------------------------------------------------
%% @doc Cluster Manager
%% Handles cluster formation and node discovery using native Erlang.
%% Supports multiple discovery strategies:
%%   - standalone: No clustering (single node)
%%   - dns: DNS-based discovery (Kubernetes headless services)
%%   - epmd: EPMD-based discovery with static hosts
%%   - gossip: UDP multicast discovery (same subnet)
%%
%% The Erlang cookie is never logged: anyone holding it can run arbitrary
%% code on every node in the cluster.
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
    gossip_port :: integer(),
    gossip_key :: binary() | undefined
}).

-define(DEFAULT_POLL_INTERVAL, 30000).
-define(DEFAULT_GOSSIP_PORT, 45892).
-define(DEFAULT_MULTICAST_ADDR, {230, 1, 1, 1}).
%% Cookies that ship in this repo's Dockerfile and compose files, so they are
%% public knowledge and must not be relied on outside local testing.
-define(EXAMPLE_COOKIES, [kraken_dev_cookie, kraken_cluster_cookie]).

-ifdef(TEST).
-export([dns_name/0, node_basename/0, node_basename/2,
         gossip_key/0, gossip_packet/2, verify_gossip_packet/2,
         cookie_status/1]).
-endif.

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
    kraken_log:info("[ClusterManager] Cookie: ~s~n", [cookie_status(erlang:get_cookie())]),

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
                gossip_port = Port,
                gossip_key = gossip_key()
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

%% Announce this node on the multicast group, then again every poll interval
%% so a node that missed earlier announces (it started later, or a packet was
%% dropped) still finds its peers.
handle_info(gossip_announce, #state{poll_interval = Interval} = State) ->
    send_gossip_announce(State),
    Timer = erlang:send_after(Interval, self(), gossip_announce),
    {noreply, State#state{poll_timer = Timer}};

%% The socket is {active, once} and re-armed after each packet, so a flood of
%% packets backs up in (and is dropped by) the kernel socket buffer instead
%% of growing this process's mailbox.
handle_info({udp, Socket, IP, _InPort, Packet},
            #state{gossip_socket = Socket, gossip_key = Key} = State) ->
    handle_gossip_message(Packet, IP, Key),
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State};

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

%% Describes the cookie without revealing it.
cookie_status(nocookie) ->
    "none (distribution is not running)";
cookie_status(Cookie) ->
    case lists:member(Cookie, ?EXAMPLE_COOKIES) of
        true -> "WARNING: using the example value from this repo; "
                "set ERLANG_COOKIE to a long random secret before exposing the "
                "Erlang distribution ports";
        false -> "custom value set"
    end.

%%====================================================================
%% Internal functions - DNS Discovery
%%====================================================================

discover_via_dns() ->
    Query = dns_name(),
    NodeBasename = node_basename(),

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

%% CLUSTER_DNS_NAME is the documented name; CLUSTER_DNS_QUERY is the name
%% earlier releases read, kept as an alias. Empty counts as unset (the
%% Dockerfile defaults CLUSTER_DNS_NAME to "").
dns_name() ->
    case env_nonempty("CLUSTER_DNS_NAME") of
        false -> case env_nonempty("CLUSTER_DNS_QUERY") of
                     false -> "";
                     Query -> Query
                 end;
        Name -> Name
    end.

%% Peers are dialled as <basename>@<ip>. The basename defaults to the name
%% part of this node's own name (kraken for kraken@10.0.0.5), since every
%% node in a cluster normally runs with the same ERLANG_NODE_NAME prefix.
node_basename() ->
    node_basename(env_nonempty("CLUSTER_NODE_BASENAME"), node()).

node_basename(false, Node) ->
    [Name | _] = string:split(atom_to_list(Node), "@"),
    Name;
node_basename(Override, _Node) ->
    Override.

env_nonempty(Var) ->
    case os:getenv(Var) of
        false -> false;
        "" -> false;
        Value -> Value
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
        {active, once},
        {reuseaddr, true},
        {multicast_ttl, 1},
        {multicast_loop, true},
        {add_membership, {MulticastAddr, {0, 0, 0, 0}}}
    ]),

    kraken_log:info("[ClusterManager] Gossip socket opened on port ~p, multicast ~p~n",
              [Port, MulticastAddr]),

    {ok, Socket, MulticastAddr, Port}.

send_gossip_announce(#state{gossip_socket = Socket, gossip_multicast_addr = Addr,
                             gossip_port = Port, gossip_key = Key}) ->
    case gen_udp:send(Socket, Addr, Port, gossip_packet(Key, node())) of
        ok -> ok;
        {error, Reason} ->
            kraken_log:info("[ClusterManager] Gossip announce failed: ~p~n", [Reason])
    end.

%% Announces are signed with HMAC-SHA256 over the node name. The key is
%% CLUSTER_GOSSIP_SECRET when set, otherwise the Erlang cookie. The secret
%% only controls which announces a node acts on: joining the cluster still
%% needs the matching cookie in the Erlang distribution handshake.
gossip_key() ->
    case env_nonempty("CLUSTER_GOSSIP_SECRET") of
        false -> atom_to_binary(erlang:get_cookie(), utf8);
        Secret -> unicode:characters_to_binary(Secret)
    end.

gossip_packet(Key, Node) ->
    NodeBin = atom_to_binary(Node, utf8),
    <<(crypto:mac(hmac, sha256, Key, NodeBin))/binary, NodeBin/binary>>.

%% Returns {ok, Node} only for a packet signed with Key that carries a
%% plausible node name; the atom is created only after the signature checks.
verify_gossip_packet(Key, <<Mac:32/binary, NodeBin/binary>>)
  when byte_size(NodeBin) > 0, byte_size(NodeBin) =< 255 ->
    case crypto:hash_equals(Mac, crypto:mac(hmac, sha256, Key, NodeBin)) of
        true ->
            case binary:split(NodeBin, <<"@">>) of
                [Name, Host] when Name =/= <<>>, Host =/= <<>> ->
                    try {ok, binary_to_atom(NodeBin, utf8)}
                    catch error:_ -> {error, bad_node_name}
                    end;
                _ ->
                    {error, bad_node_name}
            end;
        false ->
            {error, bad_signature}
    end;
verify_gossip_packet(_Key, _Packet) ->
    {error, malformed}.

handle_gossip_message(Packet, IP, Key) ->
    case verify_gossip_packet(Key, Packet) of
        {ok, Node} ->
            connect_if_not_self(Node);
        {error, Reason} ->
            kraken_log:info("[ClusterManager] Ignored gossip packet from ~s: ~p~n",
                            [inet:ntoa(IP), Reason])
    end.

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
