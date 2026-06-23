%%%-------------------------------------------------------------------
%% @doc Durable delivery tests — the delivery_store ETS backend (claim /
%% pending / cursor) and the claim-based replay flow (kraken_replay) for a
%% load-balanced group draining what it missed while scaled to zero.
%% In-memory only (no Firestore / no WS / no MQTT).
%% @end
%%%-------------------------------------------------------------------
-module(kraken_durable_delivery_tests).
-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:set_env(kraken, delivery_store_backend, ets),
    application:set_env(kraken, durable_delivery, true),
    application:set_env(kraken, store_backend, ets),
    kraken_store_ets:init(),
    kraken_delivery_store_ets:init(),
    catch ets:delete_all_objects(kraken_store_ets_messages),
    catch ets:delete_all_objects(kraken_delivery_store_ets_claims),
    catch ets:delete_all_objects(kraken_delivery_store_ets_cursors),
    ok.

seed(MsgId, Room, Ts) ->
    kraken_store_ets:log_message(#{
        message_id => MsgId,
        room_id => Room,
        topic => <<"dev/echo-room/tasks">>,
        pattern => <<"dev/echo-room/tasks">>,
        payload => msgpack:pack(#{<<"task">> => MsgId}),
        timestamp => Ts
    }).

%% Slot resolves to the ETS backend; enabled when durable_delivery is set.
backend_resolution_test() ->
    setup(),
    ?assertEqual(kraken_delivery_store_ets, kraken:backend(delivery_store)),
    ?assert(kraken_delivery_store:is_enabled()).

%% Default is no-op and disabled (OSS / non-LB unaffected).
noop_default_test() ->
    application:unset_env(kraken, delivery_store_backend),
    application:unset_env(kraken, durable_delivery),
    ?assertEqual(kraken_delivery_store_noop, kraken:backend(delivery_store)),
    ?assertNot(kraken_delivery_store:is_enabled()).

%% Atomic claim: exactly one actor wins per {group, message}.
claim_exactly_once_test() ->
    setup(),
    Base = #{app_id => <<"a">>, group_id => <<"g">>, message_id => <<"m1">>},
    ?assertEqual({ok, won},  kraken_delivery_store:claim(Base#{actor_id => <<"x">>})),
    ?assertEqual({ok, lost}, kraken_delivery_store:claim(Base#{actor_id => <<"y">>})),
    %% a different message is still claimable
    ?assertEqual({ok, won},
        kraken_delivery_store:claim(#{app_id => <<"a">>, group_id => <<"g">>,
                                      message_id => <<"m2">>, actor_id => <<"y">>})).

%% pending filters by room + cursor, oldest-first; cursor get/set round-trips.
pending_and_cursor_test() ->
    setup(),
    seed(<<"m1">>, <<"room1">>, 100),
    seed(<<"m2">>, <<"room1">>, 200),
    seed(<<"x1">>, <<"room2">>, 150),
    Q = #{app_id => <<"a">>, group_id => <<"g">>, room_id => <<"room1">>, limit => 100},
    {ok, P1} = kraken_delivery_store:pending(Q#{cursor => 0}),
    ?assertEqual([<<"m1">>, <<"m2">>], [maps:get(message_id, E) || E <- P1]),
    {ok, P2} = kraken_delivery_store:pending(Q#{cursor => 100}),
    ?assertEqual([<<"m2">>], [maps:get(message_id, E) || E <- P2]),
    ?assertEqual({ok, 0}, kraken_delivery_store:cursor_get(#{group_id => <<"g">>})),
    ok = kraken_delivery_store:cursor_set(#{group_id => <<"g">>}, 200),
    ?assertEqual({ok, 200}, kraken_delivery_store:cursor_get(#{group_id => <<"g">>})).

%% Full replay: a reconnecting group member drains the backlog (in order,
%% isReplay=true), advances the group cursor, and a second member that
%% reconnects after sees nothing left (cursor resumed past it).
replay_flow_test() ->
    setup(),
    seed(<<"m1">>, <<"room1">>, 100),
    seed(<<"m2">>, <<"room1">>, 200),
    seed(<<"m3">>, <<"room1">>, 300),
    Ctx = #{group_id => <<"g1">>, room_id => <<"room1">>},

    {ok, started} = kraken_replay:start_replay(<<"actorA">>, <<"app1">>, Ctx, self()),
    {FramesA, IdsA} = collect(<<"actorA">>, []),

    Start = hd(FramesA),
    ?assertEqual(<<"replayStart">>, maps:get(<<"type">>, Start)),
    ?assertEqual(3, maps:get(<<"count">>, Start)),
    MsgsA = [F || F <- FramesA, maps:get(<<"type">>, F) =:= <<"message">>],
    ?assertEqual([<<"m1">>, <<"m2">>, <<"m3">>], [maps:get(<<"msgId">>, F) || F <- MsgsA]),
    lists:foreach(fun(F) -> ?assertEqual(true, maps:get(<<"isReplay">>, F)) end, MsgsA),
    End = lists:last(FramesA),
    ?assertEqual(<<"replayEnd">>, maps:get(<<"type">>, End)),
    ?assertEqual(3, length(IdsA)),

    %% Second member reconnects: cursor advanced past everything -> nothing.
    {ok, started} = kraken_replay:start_replay(<<"actorB">>, <<"app1">>, Ctx, self()),
    {FramesB, IdsB} = collect(<<"actorB">>, []),
    ?assertEqual(0, maps:get(<<"count">>, hd(FramesB))),
    ?assertEqual([], IdsB).

%% Collect everything the replay worker sends until {replay_complete}.
collect(Actor, Acc) ->
    receive
        {send_to_client, M} -> collect(Actor, [M | Acc]);
        {update_replay_status, Actor, _} -> collect(Actor, Acc);
        {update_replayed_ids, Actor, _} -> collect(Actor, Acc);
        {replay_complete, Actor, Ids} -> {lists:reverse(Acc), Ids}
    after 2000 -> erlang:error({replay_timeout, lists:reverse(Acc)})
    end.
