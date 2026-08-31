%%%-------------------------------------------------------------------
%% @doc kraken_session tests — which connection owns an actor's MQTT session.
%%
%% Uses a real syn scope, because the whole point is what happens when two
%% connections for one actor are live at the same time, and that is membership
%% behaviour rather than arithmetic.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_session_tests).
-include_lib("eunit/include/eunit.hrl").

-define(ACTOR, <<"actor-1">>).

setup() ->
    application:ensure_all_started(syn),
    catch syn:add_node_to_scopes([kraken_actors]),
    catch syn:leave(kraken_actors, {actor, ?ACTOR}, self()),
    ok.

%% Run a second "connection" in its own process and keep it alive while the
%% assertion runs, since membership is what is being tested.
with_peer(ActorTokenId, Fun) ->
    Owner = self(),
    Peer = spawn(fun() ->
        Result = kraken_session:claim(ActorTokenId, true, undefined),
        Owner ! {peer_claimed, self(), Result},
        receive release -> ok after 5000 -> ok end
    end),
    receive
        {peer_claimed, Peer, PeerResult} ->
            try Fun(PeerResult) after Peer ! release end
    after 5000 ->
        erlang:error(peer_never_claimed)
    end.

%%====================================================================

%% A clean-session actor has no identity to protect and nothing to collide on.
non_persistent_actor_is_untouched_test() ->
    setup(),
    ?assertEqual({false, undefined}, kraken_session:claim(?ACTOR, false, undefined)).

%% The first connection behaves exactly as before: it owns the actor's session,
%% keyed on the actor id.
first_connection_owns_the_session_test() ->
    setup(),
    ?assertEqual({true, undefined}, kraken_session:claim(?ACTOR, true, undefined)),
    kraken_session:release(?ACTOR).

%% THE bug. A second live connection for the same actor used to ask the broker
%% for the same session, which is a takeover: the first was disconnected and
%% publishes on both stopped being acknowledged. It now gets a clean session
%% instead, which costs it nothing — before this it was kicked and had none.
second_live_connection_does_not_take_over_test() ->
    setup(),
    with_peer(?ACTOR, fun({PeerPersistent, _}) ->
        ?assertEqual(true, PeerPersistent),
        ?assertEqual({false, undefined}, kraken_session:claim(?ACTOR, true, undefined))
    end),
    kraken_session:release(?ACTOR).

%% Once the other connection is gone the actor's session is claimable again,
%% so an ordinary reconnect still resumes and drains what it missed.
session_is_claimable_again_after_release_test() ->
    setup(),
    with_peer(?ACTOR, fun(_) ->
        ?assertEqual({false, undefined}, kraken_session:claim(?ACTOR, true, undefined))
    end),
    %% the peer process has exited, so syn has dropped it
    timer:sleep(50),
    kraken_session:release(?ACTOR),
    ?assertEqual({true, undefined}, kraken_session:claim(?ACTOR, true, undefined)),
    kraken_session:release(?ACTOR).

%% Naming the instance is the way to have BOTH durability and concurrency:
%% each worker keeps its own resumable session under one token.
named_instances_each_keep_a_session_test() ->
    setup(),
    ?assertEqual({true, <<"actor-1_worker-a">>},
                 kraken_session:claim(?ACTOR, true, <<"worker-a">>)),
    with_peer(?ACTOR, fun(_) ->
        %% Even with another connection live, a named instance still gets its
        %% own session rather than being pushed to a clean one.
        ?assertEqual({true, <<"actor-1_worker-b">>},
                     kraken_session:claim(?ACTOR, true, <<"worker-b">>))
    end),
    kraken_session:release(?ACTOR).

%% The value lands inside an MQTT ClientId, so it must not carry topic
%% structure or unbounded length.
client_id_is_sanitised_test() ->
    ?assertEqual(<<"ok-worker_1">>, kraken_session:sanitise_client_id(<<"ok-worker_1">>)),
    ?assertEqual(<<"abc">>, kraken_session:sanitise_client_id(<<"a/b+c">>)),
    ?assertEqual(<<"ab">>, kraken_session:sanitise_client_id(<<"a#b">>)),
    ?assertEqual(undefined, kraken_session:sanitise_client_id(<<"///">>)),
    ?assertEqual(undefined, kraken_session:sanitise_client_id(<<>>)),
    ?assertEqual(undefined, kraken_session:sanitise_client_id(undefined)),
    ?assertEqual(undefined, kraken_session:sanitise_client_id(12345)),
    ?assertEqual(64, byte_size(kraken_session:sanitise_client_id(
        list_to_binary(lists:duplicate(200, $a))))).

%% syn not running must not stop a connection authenticating; it degrades to
%% the historical single-owner behaviour.
degrades_without_syn_test() ->
    ?assertMatch({true, _}, kraken_session:claim(<<"actor-no-syn">>, true, undefined)),
    ?assertEqual(ok, kraken_session:release(<<"actor-no-syn">>)).

%%====================================================================
%% MQTT session options (kraken_broker_mqtt)
%%====================================================================
%% The expiry has to travel as an MQTT 5 CONNECT property. emqtt speaks 3.1.1
%% by default and silently drops options it does not recognise, so the expiry
%% previously never reached the broker and sessions were kept for ever.

persistent_session_speaks_v5_with_an_expiry_test() ->
    {ClientId, CleanStart, Opts} =
        kraken_broker_mqtt:session_options(true, <<"actor-1">>, <<"actor-1">>, 3600),
    ?assertEqual(<<"kraken_agent_actor-1">>, ClientId),
    ?assertEqual(false, CleanStart),
    ?assertEqual(v5, maps:get(proto_ver, Opts)),
    ?assertEqual(#{'Session-Expiry-Interval' => 3600}, maps:get(properties, Opts)).

%% The session key, not the actor, is what names the session — that is what
%% lets two workers under one token each keep their own.
persistent_session_uses_the_session_key_test() ->
    {ClientId, _, _} =
        kraken_broker_mqtt:session_options(true, <<"actor-1">>, <<"actor-1_worker-a">>, 3600),
    ?assertEqual(<<"kraken_agent_actor-1_worker-a">>, ClientId).

%% A literal 0 in v5 ends the session at disconnect, throwing away the queue
%% this mechanism exists to keep. An unconfigured expiry means "never" instead.
unconfigured_expiry_does_not_end_the_session_test() ->
    Never = 16#FFFFFFFF,
    lists:foreach(fun(Value) ->
        {_, _, Opts} =
            kraken_broker_mqtt:session_options(true, <<"a">>, <<"a">>, Value),
        ?assertEqual(#{'Session-Expiry-Interval' => Never}, maps:get(properties, Opts))
    end, [0, -1, undefined]).

%% A clean session has no expiry to express and no reason to change protocol.
clean_session_is_unique_and_unchanged_test() ->
    {Id1, CleanStart, Opts} =
        kraken_broker_mqtt:session_options(false, <<"actor-1">>, <<"ignored">>, 3600),
    {Id2, _, _} =
        kraken_broker_mqtt:session_options(false, <<"actor-1">>, <<"ignored">>, 3600),
    ?assertEqual(true, CleanStart),
    ?assertEqual(#{}, Opts),
    ?assertNotEqual(Id1, Id2),
    ?assertMatch(<<"kraken_proxy_actor-1_", _/binary>>, Id1).
