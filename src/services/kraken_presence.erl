%%%-------------------------------------------------------------------
%% @doc Presence Service
%% Handles room-level presence using SYN registry.
%% Actors join a presence group keyed by roomId and access scope.
%% When presence is set, it propagates to associated lobbies.
%%
%% Scope isolation: topics of a scoped actor live under app/scope/room/...,
%% so tenants never see each other's messages. Presence follows the same
%% rule: the room and lobby groups are keyed by {RoomId, Scope}, where Scope
%% is the actor's scope slug (undefined for unscoped actors). The arities
%% without a Scope argument act on the unscoped groups.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_presence).

-export([
    join_room_presence/5, join_room_presence/6,
    leave_room_presence/2, leave_room_presence/3,
    update_room_presence/5, update_room_presence/6,
    get_room_presence/1, get_room_presence/2,
    broadcast_room_presence_event/3, broadcast_room_presence_event/4,
    %% Lobby functions
    join_lobby/3, join_lobby/4,
    leave_lobby/2, leave_lobby/3,
    get_lobby_presence/1, get_lobby_presence/2,
    broadcast_lobby_presence_event/4, broadcast_lobby_presence_event/5,
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
    join_room_presence(RoomId, undefined, ActorTokenId, Presence, WsPid, ProjectId).

join_room_presence(RoomId, Scope, ActorTokenId, Presence, WsPid, ProjectId) ->
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
            syn:join(kraken_presence, room_group(RoomId, Scope), WsPid, Metadata),
            %% Broadcast join event to other actors in the room
            broadcast_room_presence_event(RoomId, Scope, join, #{
                actor_token_id => ActorTokenId,
                presence => Presence
            }),
            %% Propagate to lobbies
            propagate_to_lobbies(RoomId, Scope, join, ActorTokenId, Presence),
            ok
    end.

%% Leave the room presence group
leave_room_presence(RoomId, ActorTokenId) ->
    leave_room_presence(RoomId, undefined, ActorTokenId).

leave_room_presence(RoomId, Scope, ActorTokenId) ->
    case RoomId of
        undefined ->
            ok;
        _ ->
            %% Broadcast leave event before leaving
            broadcast_room_presence_event(RoomId, Scope, leave, #{
                actor_token_id => ActorTokenId
            }),
            %% Propagate leave to lobbies
            propagate_to_lobbies(RoomId, Scope, leave, ActorTokenId, #{}),
            %% Leave the presence group
            syn:leave(kraken_presence, room_group(RoomId, Scope), self()),
            ok
    end.

%% Update presence data for an actor in a room
update_room_presence(RoomId, ActorTokenId, NewPresence, WsPid, ProjectId) ->
    update_room_presence(RoomId, undefined, ActorTokenId, NewPresence, WsPid, ProjectId).

update_room_presence(RoomId, Scope, ActorTokenId, NewPresence, WsPid, ProjectId) ->
    case RoomId of
        undefined ->
            {error, no_room_id};
        _ ->
            Group = room_group(RoomId, Scope),
            %% Check if already joined, if not join first
            case syn:is_member(kraken_presence, Group, WsPid) of
                true ->
                    %% syn 3.x: leave/rejoin to update group member metadata
                    case lists:keyfind(WsPid, 1, syn:members(kraken_presence, Group)) of
                        {_, ExistingMeta} ->
                            syn:leave(kraken_presence, Group, WsPid),
                            syn:join(kraken_presence, Group, WsPid,
                                     maps:put(presence, NewPresence, ExistingMeta));
                        false ->
                            ok
                    end;
                false ->
                    %% Join first
                    join_room_presence(RoomId, Scope, ActorTokenId, NewPresence, WsPid, ProjectId)
            end,
            %% Broadcast update event to room
            broadcast_room_presence_event(RoomId, Scope, update, #{
                actor_token_id => ActorTokenId,
                presence => NewPresence
            }),
            %% Propagate update to lobbies
            propagate_to_lobbies(RoomId, Scope, update, ActorTokenId, NewPresence),
            ok
    end.

%% Get all actors present in a room
get_room_presence(RoomId) ->
    get_room_presence(RoomId, undefined).

get_room_presence(RoomId, Scope) ->
    case RoomId of
        undefined ->
            [];
        _ ->
            Members = syn:members(kraken_presence, room_group(RoomId, Scope)),
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
    broadcast_room_presence_event(RoomId, undefined, EventType, EventData).

broadcast_room_presence_event(RoomId, Scope, EventType, EventData) ->
    Members = syn:members(kraken_presence, room_group(RoomId, Scope)),
    lists:foreach(fun({Pid, _Metadata}) ->
        Pid ! {presence_event, EventType, EventData}
    end, Members).

%%====================================================================
%% Lobby Functions
%%====================================================================

%% Join a lobby as an observer (subscribe to lobby presence events)
join_lobby(LobbyId, ActorTokenId, WsPid) ->
    join_lobby(LobbyId, undefined, ActorTokenId, WsPid).

join_lobby(LobbyId, Scope, ActorTokenId, WsPid) ->
    case LobbyId of
        undefined ->
            {error, no_lobby_id};
        _ ->
            Metadata = #{
                actor_token_id => ActorTokenId,
                subscribed_at => erlang:system_time(second)
            },
            %% Join the lobby subscriber group
            syn:join(kraken_lobbies, lobby_group(LobbyId, Scope), WsPid, Metadata),
            ok
    end.

%% Leave a lobby (unsubscribe from lobby presence events)
leave_lobby(LobbyId, ActorTokenId) ->
    leave_lobby(LobbyId, undefined, ActorTokenId).

leave_lobby(LobbyId, Scope, _ActorTokenId) ->
    case LobbyId of
        undefined ->
            ok;
        _ ->
            syn:leave(kraken_lobbies, lobby_group(LobbyId, Scope), self()),
            ok
    end.

%% Get all presence for a lobby (aggregated from all rooms)
%% Returns: #{ RoomId => #{ ActorId => PresenceData } }
get_lobby_presence(LobbyId) ->
    get_lobby_presence(LobbyId, undefined).

get_lobby_presence(LobbyId, Scope) ->
    %% Get all rooms in this lobby
    Rooms = get_rooms_for_lobby(LobbyId),
    %% For each room, get presence and build aggregated map
    lists:foldl(fun(RoomId, Acc) ->
        RoomPresence = get_room_presence(RoomId, Scope),
        %% Convert list to map keyed by actorId
        PresenceMap = lists:foldl(fun(Actor, InnerAcc) ->
            ActorId = maps:get(<<"actorTokenId">>, Actor),
            InnerAcc#{ActorId => Actor}
        end, #{}, RoomPresence),
        Acc#{RoomId => PresenceMap}
    end, #{}, Rooms).

%% Broadcast presence event to all lobby subscribers (with room context)
broadcast_lobby_presence_event(LobbyId, RoomId, EventType, EventData) ->
    broadcast_lobby_presence_event(LobbyId, RoomId, undefined, EventType, EventData).

broadcast_lobby_presence_event(LobbyId, RoomId, Scope, EventType, EventData) ->
    Members = syn:members(kraken_lobbies, lobby_group(LobbyId, Scope)),
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

%% Group names. Unscoped groups keep their original names.
room_group(RoomId, undefined) -> {room_presence, RoomId};
room_group(RoomId, Scope) -> {room_presence, RoomId, Scope}.

lobby_group(LobbyId, undefined) -> {lobby, LobbyId};
lobby_group(LobbyId, Scope) -> {lobby, LobbyId, Scope}.

%% Propagate presence event to the lobbies that contain this room, in the
%% same scope only
propagate_to_lobbies(RoomId, Scope, EventType, ActorTokenId, Presence) ->
    Lobbies = get_lobbies_for_room(RoomId),
    EventData = #{
        actor_token_id => ActorTokenId,
        presence => Presence
    },
    lists:foreach(fun(LobbyId) ->
        broadcast_lobby_presence_event(LobbyId, RoomId, Scope, EventType, EventData)
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
