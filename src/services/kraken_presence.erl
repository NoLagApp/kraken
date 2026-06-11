%%%-------------------------------------------------------------------
%% @doc Presence Service
%% Handles room-level presence using SYN registry.
%% Actors join a presence group keyed by roomId.
%% When presence is set, it propagates to associated lobbies.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_presence).

-export([
    join_room_presence/5,
    leave_room_presence/2,
    update_room_presence/5,
    get_room_presence/1,
    broadcast_room_presence_event/3,
    %% Lobby functions
    join_lobby/3,
    leave_lobby/2,
    get_lobby_presence/1,
    broadcast_lobby_presence_event/4,
    %% Internal - called by kraken_lobby_map
    get_lobbies_for_room/1
]).

%% Maximum lobbies a room can belong to (enforced by Titus, but double-check here)
-define(MAX_LOBBIES_PER_ROOM, 10).

%%====================================================================
%% Room Presence Functions
%%====================================================================

%% Join the room presence group
join_room_presence(RoomId, ActorTokenId, Presence, WsPid, ProjectId) ->
    case RoomId of
        undefined ->
            {error, no_room_id};
        _ ->
            Metadata = #{
                actor_token_id => ActorTokenId,
                project_id => ProjectId,
                presence => Presence,
                joined_at => erlang:system_time(second)
            },
            %% Join the presence group for this room
            syn:join(kraken_presence, {room_presence, RoomId}, WsPid, Metadata),
            %% Broadcast join event to other actors in the room
            broadcast_room_presence_event(RoomId, join, #{
                actor_token_id => ActorTokenId,
                presence => Presence
            }),
            %% Propagate to lobbies
            propagate_to_lobbies(RoomId, join, ActorTokenId, Presence),
            ok
    end.

%% Leave the room presence group
leave_room_presence(RoomId, ActorTokenId) ->
    case RoomId of
        undefined ->
            ok;
        _ ->
            %% Broadcast leave event before leaving
            broadcast_room_presence_event(RoomId, leave, #{
                actor_token_id => ActorTokenId
            }),
            %% Propagate leave to lobbies
            propagate_to_lobbies(RoomId, leave, ActorTokenId, #{}),
            %% Leave the presence group
            syn:leave(kraken_presence, {room_presence, RoomId}, self()),
            ok
    end.

%% Update presence data for an actor in a room
update_room_presence(RoomId, ActorTokenId, NewPresence, WsPid, ProjectId) ->
    case RoomId of
        undefined ->
            {error, no_room_id};
        _ ->
            %% Check if already joined, if not join first
            case syn:is_member(kraken_presence, {room_presence, RoomId}, WsPid) of
                true ->
                    %% syn 3.x: leave/rejoin to update group member metadata
                    case lists:keyfind(WsPid, 1, syn:members(kraken_presence, {room_presence, RoomId})) of
                        {_, ExistingMeta} ->
                            syn:leave(kraken_presence, {room_presence, RoomId}, WsPid),
                            syn:join(kraken_presence, {room_presence, RoomId}, WsPid,
                                     maps:put(presence, NewPresence, ExistingMeta));
                        false ->
                            ok
                    end;
                false ->
                    %% Join first
                    join_room_presence(RoomId, ActorTokenId, NewPresence, WsPid, ProjectId)
            end,
            %% Broadcast update event to room
            broadcast_room_presence_event(RoomId, update, #{
                actor_token_id => ActorTokenId,
                presence => NewPresence
            }),
            %% Propagate update to lobbies
            propagate_to_lobbies(RoomId, update, ActorTokenId, NewPresence),
            ok
    end.

%% Get all actors present in a room
get_room_presence(RoomId) ->
    case RoomId of
        undefined ->
            [];
        _ ->
            Members = syn:members(kraken_presence, {room_presence, RoomId}),
            lists:map(fun({_Pid, Metadata}) ->
                #{
                    <<"actorTokenId">> => maps:get(actor_token_id, Metadata, null),
                    <<"presence">> => maps:get(presence, Metadata, null),
                    <<"joinedAt">> => maps:get(joined_at, Metadata, null)
                }
            end, Members)
    end.

%% Broadcast presence event to all actors in room
broadcast_room_presence_event(RoomId, EventType, EventData) ->
    Members = syn:members(kraken_presence, {room_presence, RoomId}),
    lists:foreach(fun({Pid, _Metadata}) ->
        Pid ! {presence_event, EventType, EventData}
    end, Members).

%%====================================================================
%% Lobby Functions
%%====================================================================

%% Join a lobby as an observer (subscribe to lobby presence events)
join_lobby(LobbyId, ActorTokenId, WsPid) ->
    case LobbyId of
        undefined ->
            {error, no_lobby_id};
        _ ->
            Metadata = #{
                actor_token_id => ActorTokenId,
                subscribed_at => erlang:system_time(second)
            },
            %% Join the lobby subscriber group
            syn:join(kraken_lobbies, {lobby, LobbyId}, WsPid, Metadata),
            ok
    end.

%% Leave a lobby (unsubscribe from lobby presence events)
leave_lobby(LobbyId, _ActorTokenId) ->
    case LobbyId of
        undefined ->
            ok;
        _ ->
            syn:leave(kraken_lobbies, {lobby, LobbyId}, self()),
            ok
    end.

%% Get all presence for a lobby (aggregated from all rooms)
%% Returns: #{ RoomId => #{ ActorId => PresenceData } }
get_lobby_presence(LobbyId) ->
    %% Get all rooms in this lobby
    Rooms = get_rooms_for_lobby(LobbyId),
    %% For each room, get presence and build aggregated map
    lists:foldl(fun(RoomId, Acc) ->
        RoomPresence = get_room_presence(RoomId),
        %% Convert list to map keyed by actorId
        PresenceMap = lists:foldl(fun(Actor, InnerAcc) ->
            ActorId = maps:get(<<"actorTokenId">>, Actor),
            InnerAcc#{ActorId => Actor}
        end, #{}, RoomPresence),
        Acc#{RoomId => PresenceMap}
    end, #{}, Rooms).

%% Broadcast presence event to all lobby subscribers (with room context)
broadcast_lobby_presence_event(LobbyId, RoomId, EventType, EventData) ->
    Members = syn:members(kraken_lobbies, {lobby, LobbyId}),
    EventWithRoom = EventData#{
        room_id => RoomId,
        lobby_id => LobbyId
    },
    lists:foreach(fun({Pid, _Metadata}) ->
        Pid ! {lobby_presence_event, EventType, EventWithRoom}
    end, Members).

%%====================================================================
%% Internal Functions
%%====================================================================

%% Propagate presence event to all lobbies that contain this room
propagate_to_lobbies(RoomId, EventType, ActorTokenId, Presence) ->
    Lobbies = get_lobbies_for_room(RoomId),
    EventData = #{
        actor_token_id => ActorTokenId,
        presence => Presence
    },
    lists:foreach(fun(LobbyId) ->
        broadcast_lobby_presence_event(LobbyId, RoomId, EventType, EventData)
    end, Lobbies).

%% Get lobbies for a room (from cache or Titus API)
%% TODO: Implement caching via kraken_lobby_map module
get_lobbies_for_room(RoomId) ->
    case kraken_lobby_map:get_lobbies(RoomId) of
        {ok, Lobbies} ->
            Lobbies;
        {error, _} ->
            %% Cache miss or error - return empty for now
            %% In production, this would fetch from Titus API
            []
    end.

%% Get rooms for a lobby (from cache or Titus API)
%% TODO: Implement caching via kraken_lobby_map module
get_rooms_for_lobby(LobbyId) ->
    case kraken_lobby_map:get_rooms(LobbyId) of
        {ok, Rooms} ->
            Rooms;
        {error, _} ->
            %% Cache miss or error - return empty for now
            []
    end.
