%%%-------------------------------------------------------------------
%% @doc Room presence is partitioned by access scope, like topics are.
%%
%% Actors in different scopes can hold grants for the same room id (with
%% @nolag/core every scope shares its app's rooms); they must not see each
%% other's presence.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_presence_scope_tests).
-include_lib("eunit/include/eunit.hrl").

-define(ROOM, <<"room-general">>).

setup() ->
    application:ensure_all_started(syn),
    catch syn:add_node_to_scopes([kraken_presence, kraken_lobbies]),
    case whereis(kraken_lobby_map) of
        undefined -> {ok, _} = kraken_lobby_map:start_link(), ok;
        _ -> ok
    end,
    ok.

%% A "connection" that joins room presence and stays until released,
%% forwarding presence events to the test process.
member(Scope, Actor) ->
    Owner = self(),
    Pid = spawn(fun() ->
        kraken_presence:update_room_presence(?ROOM, Scope, Actor, #{<<"n">> => Actor}, self(), <<"p">>),
        Owner ! {joined, self()},
        loop(Owner, Actor)
    end),
    receive {joined, Pid} -> Pid after 5000 -> erlang:error(never_joined) end.

loop(Owner, Actor) ->
    receive
        stop -> ok;
        {presence_event, Type, Data} ->
            Owner ! {event, Actor, Type, maps:get(actor_token_id, Data, undefined)},
            loop(Owner, Actor)
    after 5000 -> ok
    end.

actors(Scope) ->
    lists:sort([maps:get(<<"actorTokenId">>, P) || P <- kraken_presence:get_room_presence(?ROOM, Scope)]).

flush() ->
    receive {event, _, _, _} -> flush() after 50 -> ok end.

events_for(Actor) ->
    receive {event, Actor, Type, From} -> [{Type, From} | events_for(Actor)] after 100 -> [] end.

%%====================================================================

scopes_do_not_see_each_other_test() ->
    setup(),
    A1 = member(<<"acme">>, <<"acme-1">>),
    A2 = member(<<"acme">>, <<"acme-2">>),
    G1 = member(<<"globex">>, <<"globex-1">>),
    U1 = member(undefined, <<"unscoped-1">>),
    try
        ?assertEqual([<<"acme-1">>, <<"acme-2">>], actors(<<"acme">>)),
        ?assertEqual([<<"globex-1">>], actors(<<"globex">>)),
        ?assertEqual([<<"unscoped-1">>], actors(undefined)),
        %% The two-argument form is the unscoped room
        ?assertEqual(actors(undefined),
                     lists:sort([maps:get(<<"actorTokenId">>, P) || P <- kraken_presence:get_room_presence(?ROOM)]))
    after
        [P ! stop || P <- [A1, A2, G1, U1]]
    end.

events_stay_inside_the_scope_test() ->
    setup(),
    A1 = member(<<"acme">>, <<"acme-1">>),
    flush(),
    G1 = member(<<"globex">>, <<"globex-1">>),
    A2 = member(<<"acme">>, <<"acme-2">>),
    try
        Seen = events_for(<<"acme-1">>),
        ?assert(lists:member({update, <<"acme-2">>}, Seen) orelse lists:member({join, <<"acme-2">>}, Seen)),
        ?assertNot(lists:keymember(<<"globex-1">>, 2, Seen))
    after
        [P ! stop || P <- [A1, A2, G1]]
    end.
