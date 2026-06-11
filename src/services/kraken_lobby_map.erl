%%%-------------------------------------------------------------------
%% @doc Lobby Cache Service
%% Caches lobby-room mappings provided by auth backends (warm_cache).
%% Uses ETS for fast in-memory lookups.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_lobby_map).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    get_lobbies/1,
    get_rooms/1,
    invalidate_room/1,
    invalidate_lobby/1,
    warm_cache/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(ROOM_LOBBIES_TABLE, kraken_lobby_map_room_lobbies).
-define(LOBBY_ROOMS_TABLE, kraken_lobby_map_lobby_rooms).
-define(CACHE_TTL_MS, 300000). % 5 minutes

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Get lobbies for a room (returns cached or fetches from Titus)
-spec get_lobbies(RoomId :: binary()) -> {ok, [binary()]} | {error, term()}.
get_lobbies(RoomId) ->
    case ets:lookup(?ROOM_LOBBIES_TABLE, RoomId) of
        [{RoomId, Lobbies, ExpiresAt}] ->
            Now = erlang:system_time(millisecond),
            case Now < ExpiresAt of
                true ->
                    {ok, Lobbies};
                false ->
                    %% Cache expired, fetch fresh
                    gen_server:call(?SERVER, {fetch_lobbies_for_room, RoomId})
            end;
        [] ->
            %% Cache miss, fetch from Titus
            gen_server:call(?SERVER, {fetch_lobbies_for_room, RoomId})
    end.

%% Get rooms for a lobby (returns cached or fetches from Titus)
-spec get_rooms(LobbyId :: binary()) -> {ok, [binary()]} | {error, term()}.
get_rooms(LobbyId) ->
    case ets:lookup(?LOBBY_ROOMS_TABLE, LobbyId) of
        [{LobbyId, Rooms, ExpiresAt}] ->
            Now = erlang:system_time(millisecond),
            case Now < ExpiresAt of
                true ->
                    {ok, Rooms};
                false ->
                    %% Cache expired, fetch fresh
                    gen_server:call(?SERVER, {fetch_rooms_for_lobby, LobbyId})
            end;
        [] ->
            %% Cache miss, fetch from Titus
            gen_server:call(?SERVER, {fetch_rooms_for_lobby, LobbyId})
    end.

%% Invalidate cache for a room (called when room-lobby membership changes)
-spec invalidate_room(RoomId :: binary()) -> ok.
invalidate_room(RoomId) ->
    ets:delete(?ROOM_LOBBIES_TABLE, RoomId),
    ok.

%% Invalidate cache for a lobby (called when lobby-room membership changes)
-spec invalidate_lobby(LobbyId :: binary()) -> ok.
invalidate_lobby(LobbyId) ->
    ets:delete(?LOBBY_ROOMS_TABLE, LobbyId),
    ok.

%% Warm cache with known mappings (called on startup or after bulk operations)
-spec warm_cache(RoomLobbies :: [{binary(), [binary()]}], LobbyRooms :: [{binary(), [binary()]}]) -> ok.
warm_cache(RoomLobbies, LobbyRooms) ->
    gen_server:cast(?SERVER, {warm_cache, RoomLobbies, LobbyRooms}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables for caching
    ets:new(?ROOM_LOBBIES_TABLE, [named_table, public, {read_concurrency, true}]),
    ets:new(?LOBBY_ROOMS_TABLE, [named_table, public, {read_concurrency, true}]),

    kraken_log:info("[LobbyCache] Started (warmed from auth results)", []),
    {ok, #{}}.

%% No external fetch: lobby-room mappings are provided by the auth
%% backend (room_lobby data in auth results) via warm_cache. A miss
%% simply means the actor's auth carried no lobby mapping.
handle_call({fetch_lobbies_for_room, _RoomId}, _From, State) ->
    {reply, {ok, []}, State};

handle_call({fetch_rooms_for_lobby, _LobbyId}, _From, State) ->
    {reply, {ok, []}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({warm_cache, RoomLobbies, LobbyRooms}, State) ->
    ExpiresAt = erlang:system_time(millisecond) + ?CACHE_TTL_MS,
    lists:foreach(fun({RoomId, Lobbies}) ->
        ets:insert(?ROOM_LOBBIES_TABLE, {RoomId, Lobbies, ExpiresAt})
    end, RoomLobbies),
    lists:foreach(fun({LobbyId, Rooms}) ->
        ets:insert(?LOBBY_ROOMS_TABLE, {LobbyId, Rooms, ExpiresAt})
    end, LobbyRooms),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% Hot code upgrade callback
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
