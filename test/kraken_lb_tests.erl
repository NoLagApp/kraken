%%%-------------------------------------------------------------------
%% @doc kraken_lb tests — releasing shared subscriptions on disconnect, and
%% claiming a load-balanced message when it is delivered live so a later
%% replay does not hand the same work to the next member that joins.
%%
%% In-memory only (no Firestore / no WS / no MQTT). The broker is
%% lb_test_broker, which records its unsubscribes.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_lb_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TOPIC, <<"app/echo-workers/tasks">>).
-define(GROUP, <<"proj_app_pool">>).
-define(ROOM, <<"room1">>).
-define(APP, <<"app1">>).

setup() ->
    application:set_env(kraken, delivery_store_backend, ets),
    application:set_env(kraken, durable_delivery, true),
    application:set_env(kraken, store_backend, ets),
    application:set_env(kraken, broker_backend, lb_test_broker),
    application:unset_env(kraken, release_shared_subs_on_close),
    kraken_store_ets:init(),
    kraken_delivery_store_ets:init(),
    lb_test_broker:reset(),
    catch ets:delete_all_objects(kraken_store_ets_messages),
    catch ets:delete_all_objects(kraken_delivery_store_ets_claims),
    catch ets:delete_all_objects(kraken_delivery_store_ets_cursors),
    %% The module reads the CALLING process's dictionary, so clear anything a
    %% previous test in this process left behind.
    [erase(K) || {K, _} <- get(), is_tuple(K),
                 element(1, K) =:= lb_subscription orelse
                 element(1, K) =:= mqtt_topics_for],
    ok.

seed(MsgId, Room, Ts) ->
    kraken_store_ets:log_message(#{
        message_id => MsgId,
        room_id => Room,
        topic => ?TOPIC,
        pattern => ?TOPIC,
        payload => msgpack:pack(#{<<"task">> => MsgId}),
        timestamp => Ts
    }).

%% claim_live detaches its store write, so wait for it rather than assume.
await_claim(MsgId) ->
    await_claim(MsgId, 200).

await_claim(_MsgId, 0) ->
    erlang:error(claim_never_written);
await_claim(MsgId, N) ->
    case ets:lookup(kraken_delivery_store_ets_claims, {?GROUP, MsgId}) of
        [] -> timer:sleep(10), await_claim(MsgId, N - 1);
        [_ | _] -> ok
    end.

%%====================================================================
%% Releasing shared subscriptions on disconnect
%%====================================================================

%% A persistent session keeps its subscriptions across a disconnect, so the
%% shared ones have to be handed back or the broker keeps round-robining work
%% to a member that is gone. Plain subscriptions are left alone: they are not
%% part of any group and re-subscribing costs a round trip.
release_unsubscribes_only_shared_test() ->
    setup(),
    put({mqtt_topics_for, ?TOPIC},
        [<<"$share/", ?GROUP/binary, "/room1/tasks/#">>]),
    put({mqtt_topics_for, <<"app/room/chat">>}, [<<"room1/chat/#">>]),

    ok = kraken_lb:release_shared_subscriptions(session, true),

    ?assertEqual([<<"$share/", ?GROUP/binary, "/room1/tasks/#">>],
                 lb_test_broker:unsubscribed()).

%% A clean session drops its subscriptions on disconnect anyway; releasing
%% them would be a round trip per topic on every close for nothing.
release_skips_clean_session_test() ->
    setup(),
    put({mqtt_topics_for, ?TOPIC}, [<<"$share/", ?GROUP/binary, "/room1/tasks/#">>]),

    ok = kraken_lb:release_shared_subscriptions(session, false),

    ?assertEqual([], lb_test_broker:unsubscribed()).

%% Without a delivery store behind it, the retained subscription IS the
%% scale-to-zero backlog. Releasing it there would turn a distribution bug
%% into lost work, so those deployments keep the old behaviour.
release_inert_when_durable_off_test() ->
    setup(),
    application:set_env(kraken, durable_delivery, false),
    put({mqtt_topics_for, ?TOPIC}, [<<"$share/", ?GROUP/binary, "/room1/tasks/#">>]),

    ok = kraken_lb:release_shared_subscriptions(session, true),

    ?assertEqual([], lb_test_broker:unsubscribed()),
    application:set_env(kraken, durable_delivery, true).

release_kill_switch_test() ->
    setup(),
    application:set_env(kraken, release_shared_subs_on_close, false),
    put({mqtt_topics_for, ?TOPIC}, [<<"$share/", ?GROUP/binary, "/room1/tasks/#">>]),

    ok = kraken_lb:release_shared_subscriptions(session, true),

    ?assertEqual([], lb_test_broker:unsubscribed()),
    application:unset_env(kraken, release_shared_subs_on_close).

%% terminate/3 runs when the client is frequently already gone, and the MQTT
%% backend hard-matches emqtt's reply. One dead topic must not abort the rest
%% of teardown.
release_survives_dead_broker_test() ->
    setup(),
    lb_test_broker:fail(true),
    put({mqtt_topics_for, ?TOPIC}, [<<"$share/", ?GROUP/binary, "/room1/tasks/#">>]),

    ?assertEqual(ok, kraken_lb:release_shared_subscriptions(session, true)).

%%====================================================================
%% Claiming live deliveries
%%====================================================================

%% THE regression test. A live member consumes the work; a second member then
%% joins the group. Before the claim existed, replay handed that member every
%% message since the group cursor — the work the first member had already
%% done.
claim_live_prevents_replay_of_consumed_work_test() ->
    setup(),
    seed(<<"m1">>, ?ROOM, 100),
    seed(<<"m2">>, ?ROOM, 200),
    ok = kraken_delivery_store:cursor_set(#{group_id => ?GROUP}, 50),

    %% Member X is subscribed load-balanced and receives both messages live.
    kraken_lb:remember_subscription(?TOPIC, ?ROOM, ?APP, ?GROUP),
    kraken_lb:claim_live(<<"m1">>, ?TOPIC, <<"actorX">>),
    kraken_lb:claim_live(<<"m2">>, ?TOPIC, <<"actorX">>),
    await_claim(<<"m1">>),
    await_claim(<<"m2">>),

    %% Member Y joins the same group and replays.
    Ctx = #{group_id => ?GROUP, room_id => ?ROOM, topic => ?TOPIC},
    {ok, started} = kraken_replay:start_replay(<<"actorY">>, ?APP, Ctx, self()),
    {Frames, Ids} = collect(<<"actorY">>, []),

    ?assertEqual(0, maps:get(<<"count">>, hd(Frames))),
    ?assertEqual([], Ids).

%% Control for the test above: without the claim the same setup replays
%% everything, which is exactly the bug that was measured.
claim_live_is_what_prevents_it_test() ->
    setup(),
    seed(<<"m1">>, ?ROOM, 100),
    seed(<<"m2">>, ?ROOM, 200),
    ok = kraken_delivery_store:cursor_set(#{group_id => ?GROUP}, 50),

    Ctx = #{group_id => ?GROUP, room_id => ?ROOM, topic => ?TOPIC},
    {ok, started} = kraken_replay:start_replay(<<"actorY">>, ?APP, Ctx, self()),
    {Frames, Ids} = collect(<<"actorY">>, []),

    ?assertEqual(2, maps:get(<<"count">>, hd(Frames))),
    ?assertEqual(2, length(Ids)).

%% A message that did not arrive through a load-balanced subscription has no
%% group to claim it for, and must not be claimed under someone else's.
claim_live_ignores_non_lb_test() ->
    setup(),
    seed(<<"m1">>, ?ROOM, 100),
    ok = kraken_delivery_store:cursor_set(#{group_id => ?GROUP}, 50),

    %% No remember_subscription: this topic is not load balanced.
    kraken_lb:claim_live(<<"m1">>, ?TOPIC, <<"actorX">>),
    timer:sleep(50),

    Ctx = #{group_id => ?GROUP, room_id => ?ROOM, topic => ?TOPIC},
    {ok, started} = kraken_replay:start_replay(<<"actorY">>, ?APP, Ctx, self()),
    {Frames, _} = collect(<<"actorY">>, []),
    ?assertEqual(1, maps:get(<<"count">>, hd(Frames))).

%% No message id means recording is off, so nothing was stored and there is
%% nothing to replay either.
claim_live_noop_without_msgid_test() ->
    setup(),
    kraken_lb:remember_subscription(?TOPIC, ?ROOM, ?APP, ?GROUP),
    ?assertEqual(ok, kraken_lb:claim_live(undefined, ?TOPIC, <<"actorX">>)),
    ?assertEqual([], ets:tab2list(kraken_delivery_store_ets_claims)).

claim_live_inert_when_durable_off_test() ->
    setup(),
    application:set_env(kraken, durable_delivery, false),
    kraken_lb:remember_subscription(?TOPIC, ?ROOM, ?APP, ?GROUP),

    ?assertEqual(ok, kraken_lb:claim_live(<<"m1">>, ?TOPIC, <<"actorX">>)),
    timer:sleep(50),
    ?assertEqual([], ets:tab2list(kraken_delivery_store_ets_claims)),
    application:set_env(kraken, durable_delivery, true).

%% Once live deliveries are claimed, a busy group can fill a whole replay page
%% with messages that are already taken. Stopping at that page would emit
%% nothing and strand the genuinely missed work behind it, and
%% {replay_started, GroupId} means the connection would not try again.
replay_pages_past_a_fully_claimed_window_test() ->
    setup(),
    kraken_lb:remember_subscription(?TOPIC, ?ROOM, ?APP, ?GROUP),
    ok = kraken_delivery_store:cursor_set(#{group_id => ?GROUP}, 50),

    %% 12 consumed live, then 3 that nobody took.
    %% Distinct timestamps: the cursor is a watermark, so ties would be
    %% skipped rather than paged and the test would not prove anything.
    Consumed = [{list_to_binary("c" ++ integer_to_list(N)), 100 + N} || N <- lists:seq(1, 12)],
    lists:foreach(fun({Id, Ts}) -> seed(Id, ?ROOM, Ts) end, Consumed),
    lists:foreach(fun({Id, _}) -> kraken_lb:claim_live(Id, ?TOPIC, <<"actorX">>) end, Consumed),
    lists:foreach(fun({Id, _}) -> await_claim(Id) end, Consumed),
    Missed = [{<<"missed1">>, 201}, {<<"missed2">>, 202}, {<<"missed3">>, 203}],
    lists:foreach(fun({Id, Ts}) -> seed(Id, ?ROOM, Ts) end, Missed),

    %% Page size 5: the first two pages are entirely claimed work.
    Ctx = #{group_id => ?GROUP, room_id => ?ROOM, topic => ?TOPIC, limit => 5},
    {ok, started} = kraken_replay:start_replay(<<"actorY">>, ?APP, Ctx, self()),
    {Frames, Ids} = collect(<<"actorY">>, []),

    ?assertEqual(3, maps:get(<<"count">>, hd(Frames))),
    ?assertEqual(lists:sort([Id || {Id, _} <- Missed]), lists:sort(Ids)).

%%====================================================================
%% Subscription context
%%====================================================================

remember_subscription_records_context_test() ->
    setup(),
    kraken_lb:remember_subscription(?TOPIC, ?ROOM, ?APP, ?GROUP),
    ?assertEqual(#{group_id => ?GROUP, room_id => ?ROOM,
                   app_id => ?APP, topic => ?TOPIC},
                 get({lb_subscription, ?TOPIC})).

%% A wildcard subscription resolves to no room, and the backlog query is
%% room-scoped, so there is nothing meaningful to track.
remember_subscription_needs_a_room_test() ->
    setup(),
    kraken_lb:remember_subscription(?TOPIC, undefined, ?APP, ?GROUP),
    ?assertEqual(undefined, get({lb_subscription, ?TOPIC})).

%% Keeping the context after an unsubscribe would arm a replay for a group
%% this connection has left.
forget_subscription_clears_context_test() ->
    setup(),
    kraken_lb:remember_subscription(?TOPIC, ?ROOM, ?APP, ?GROUP),
    ok = kraken_lb:forget_subscription(?TOPIC),
    ?assertEqual(undefined, get({lb_subscription, ?TOPIC})).

%% Every LB subscription's group cursor is stamped on disconnect, so a
%% returning member replays only the window it was away for.
mark_offline_boundary_stamps_cursor_test() ->
    setup(),
    kraken_lb:remember_subscription(?TOPIC, ?ROOM, ?APP, ?GROUP),
    ok = kraken_delivery_store:cursor_set(#{group_id => ?GROUP}, 50),

    ok = kraken_lb:mark_offline_boundary(),

    {ok, Cursor} = kraken_delivery_store:cursor_get(#{group_id => ?GROUP}),
    ?assert(Cursor > 50).

%%====================================================================

collect(Actor, Acc) ->
    receive
        {send_to_client, M} -> collect(Actor, [M | Acc]);
        {update_replay_status, Actor, _} -> collect(Actor, Acc);
        {update_replayed_ids, Actor, _} -> collect(Actor, Acc);
        {replay_complete, Actor, Ids} -> {lists:reverse(Acc), Ids}
    after 2000 -> erlang:error({replay_timeout, lists:reverse(Acc)})
    end.
