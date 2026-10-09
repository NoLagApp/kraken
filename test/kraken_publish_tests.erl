%%%-------------------------------------------------------------------
%% @doc The shared publish pipeline: envelope, sender stamping, filters,
%% limits — and the trigger request it hands to the webhook service.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_publish_tests).

-include_lib("eunit/include/eunit.hrl").

-define(ACTOR, #{type => actor, id => <<"actor-1">>, actor_type => <<"user">>}).

ctx(Overrides) ->
    maps:merge(#{
        sender => ?ACTOR,
        organization_id => <<"org-1">>,
        project_id => <<"proj-1">>,
        app => undefined,
        app_id => <<"app-1">>,
        room_id => undefined,
        broker_topic => <<"scope-1/room-1/messages">>,
        internal_topic => <<"scope-1/room-1/messages">>,
        pattern => <<"chat/land-a/thread/messages">>,
        room_name => <<"thread">>,
        scope => undefined,
        broker_session => #{},
        echo_sender => undefined,
        store => undefined,
        max_message_size => 921600,
        fire_webhooks => false
    }, Overrides).

publish_test_() ->
    {setup,
     fun() ->
         application:set_env(kraken, broker_backend, lb_test_broker),
         application:set_env(kraken, store_backend, kraken_store_ets),
         kraken_store_ets:init()
     end,
     fun(_) ->
         application:unset_env(kraken, broker_backend),
         application:unset_env(kraken, store_backend)
     end,
     [
      fun envelope_without_recording/0,
      fun envelope_with_recording/0,
      fun client_cannot_forge_sender/0,
      fun server_sender/0,
      fun single_filter_is_appended/0,
      fun filters_are_an_and_composite/0,
      fun invalid_filters_are_rejected/0,
      fun too_large_is_rejected/0,
      fun echo_false_passes_the_connection/0
     ]}.

last_publish() ->
    lists:last(lb_test_broker:published()).

envelope_without_recording() ->
    lb_test_broker:reset(),
    {ok, MsgId} = kraken_publish:publish(ctx(#{}), #{<<"data">> => #{<<"text">> => <<"hi">>}}),
    ?assert(is_binary(MsgId)),
    {Topic, Payload, Sender, QoS, Retain} = last_publish(),
    ?assertEqual(<<"scope-1/room-1/messages">>, Topic),
    %% Not recorded: no _msgId for subscribers to ack.
    ?assertEqual(#{<<"_data">> => #{<<"text">> => <<"hi">>},
                   <<"_from">> => #{<<"type">> => <<"actor">>, <<"id">> => <<"actor-1">>}},
                 Payload),
    ?assertEqual(undefined, Sender),
    ?assertEqual(1, QoS),
    ?assertEqual(false, Retain).

envelope_with_recording() ->
    lb_test_broker:reset(),
    {ok, MsgId} = kraken_publish:publish(ctx(#{store => enabled}),
                                         #{<<"data">> => #{<<"n">> => 1}}),
    {_, Payload, _, _, _} = last_publish(),
    ?assertEqual(MsgId, maps:get(<<"_msgId">>, Payload)),
    [{MsgId, Doc}] = ets:lookup(kraken_store_ets_messages, MsgId),
    ?assertEqual(<<"actor-1">>, maps:get(sender_actor_id, Doc)),
    ?assertEqual(<<"actor">>, maps:get(sender_type, Doc)),
    ?assertEqual(<<"app-1">>, maps:get(app_id, Doc)).

client_cannot_forge_sender() ->
    lb_test_broker:reset(),
    Forged = #{<<"_from">> => #{<<"type">> => <<"server">>, <<"id">> => <<"key">>},
               <<"_data">> => <<"pretend">>},
    {ok, _} = kraken_publish:publish(ctx(#{}), #{<<"data">> => Forged}),
    {_, Payload, _, _, _} = last_publish(),
    %% What the client sent is data; the sender is the broker's.
    {_MsgId, From, Data} = kraken_publish:unwrap(Payload),
    ?assertEqual(#{<<"type">> => <<"actor">>, <<"id">> => <<"actor-1">>}, From),
    ?assertEqual(Forged, Data).

server_sender() ->
    lb_test_broker:reset(),
    {ok, _} = kraken_publish:publish(ctx(#{sender => #{type => server, id => <<"key-9">>}}),
                                     #{<<"data">> => #{}}),
    {_, Payload, _, _, _} = last_publish(),
    {_, From, _} = kraken_publish:unwrap(Payload),
    ?assertEqual(#{<<"from">> => <<"key-9">>, <<"fromType">> => <<"server">>},
                 kraken_publish:sender_fields(From)).

single_filter_is_appended() ->
    lb_test_broker:reset(),
    {ok, _} = kraken_publish:publish(ctx(#{}), #{<<"data">> => 1, <<"filter">> => <<"Thread-7">>}),
    {Topic, _, _, _, _} = last_publish(),
    %% A single filter is used as-is, exactly as subscribers match it.
    ?assertEqual(<<"scope-1/room-1/messages/Thread-7">>, Topic).

filters_are_an_and_composite() ->
    lb_test_broker:reset(),
    {ok, _} = kraken_publish:publish(ctx(#{}),
                                     #{<<"data">> => 1, <<"filters">> => [<<"Zeta">>, <<"alpha">>]}),
    {Topic, _, _, _, _} = last_publish(),
    ?assertEqual(<<"scope-1/room-1/messages/alpha|zeta">>, Topic).

invalid_filters_are_rejected() ->
    lb_test_broker:reset(),
    [?assertMatch({error, {42960, <<"invalid_filter">>, _}},
                  kraken_publish:publish(ctx(#{}), Msg#{<<"data">> => 1}))
     || Msg <- [#{<<"filter">> => <<"a/b">>},
                #{<<"filter">> => <<"#">>},
                #{<<"filters">> => [<<"ok">>, <<"no+">>]},
                #{<<"filters">> => [<<"ok">>, 7]}]],
    ?assertEqual([], lb_test_broker:published()).

too_large_is_rejected() ->
    lb_test_broker:reset(),
    ?assertMatch({error, {42930, <<"message_too_large">>, #{<<"maxSizeBytes">> := 8}}},
                 kraken_publish:publish(ctx(#{max_message_size => 8}),
                                        #{<<"data">> => <<"well over eight bytes">>})),
    ?assertEqual([], lb_test_broker:published()).

echo_false_passes_the_connection() ->
    lb_test_broker:reset(),
    {ok, _} = kraken_publish:publish(ctx(#{echo_sender => <<"conn-1">>}), #{<<"data">> => 1}),
    {_, _, Sender, _, _} = last_publish(),
    ?assertEqual(<<"conn-1">>, Sender).

%%====================================================================
%% unwrap — current, older and echo=false envelopes
%%====================================================================

unwrap_test_() ->
    From = #{<<"type">> => <<"actor">>, <<"id">> => <<"a">>},
    [
     ?_assertEqual({<<"m">>, From, <<"d">>},
                   kraken_publish:unwrap(#{<<"_msgId">> => <<"m">>, <<"_data">> => <<"d">>,
                                           <<"_from">> => From})),
     ?_assertEqual({undefined, From, <<"d">>},
                   kraken_publish:unwrap(#{<<"_data">> => <<"d">>, <<"_from">> => From})),
     %% An older node's recorded envelope.
     ?_assertEqual({<<"m">>, undefined, <<"d">>},
                   kraken_publish:unwrap(#{<<"_msgId">> => <<"m">>, <<"_data">> => <<"d">>})),
     %% An older node's unrecorded publish: raw data.
     ?_assertEqual({undefined, undefined, #{<<"x">> => 1}},
                   kraken_publish:unwrap(#{<<"x">> => 1})),
     %% echo=false keeps the _sender wrapper around the unwrapped data.
     ?_assertEqual({<<"m">>, From, #{<<"_sender">> => <<"c">>, <<"data">> => <<"d">>}},
                   kraken_publish:unwrap(#{<<"_sender">> => <<"c">>,
                                           <<"data">> => #{<<"_msgId">> => <<"m">>,
                                                           <<"_data">> => <<"d">>,
                                                           <<"_from">> => From}})),
     ?_assertEqual(#{}, kraken_publish:sender_fields(undefined))
    ].

%%====================================================================
%% Trigger webhooks
%%====================================================================

event(Overrides) ->
    maps:merge(#{
        message_id => <<"msg-1">>,
        organization_id => <<"org-1">>,
        project_id => <<"proj-1">>,
        app_id => <<"app-1">>,
        room_id => <<"room-1">>,
        room_name => <<"thread">>,
        topic_name => <<"messages">>,
        filter => undefined,
        scope => #{id => <<"scope-1">>, slug => <<"land-a">>, name => <<"Land A">>},
        sender => ?ACTOR,
        signing_secrets => [],
        data => #{<<"text">> => <<"hi">>}
    }, Overrides).

signature_vector_test() ->
    %% Independently computed: HMAC-SHA256 over "<t>.<body>", lowercase hex.
    ?assertEqual(
        <<"t=1700000000,"
          "v1=38877139021993b830af32feea6e18a8da83eb2f6e49ee50bd9e4cf4ca4d3789,"
          "v1=b9d801502bb535e960ee9aeee7a294b082bda5089d79c7db610b85116ce6faa6">>,
        kraken_webhooks:signature_header([<<"whsec_test">>, <<"whsec_old">>],
                                         <<"1700000000">>, <<"{\"a\":1}">>)).

unsigned_trigger_keeps_the_legacy_body_test() ->
    {Headers, Body} = kraken_webhooks:trigger_request(
        #{<<"url">> => <<"http://x">>, <<"headers">> => #{<<"x-key">> => <<"k">>}}, event(#{})),
    ?assertEqual([{"x-key", "k"}], Headers),
    ?assertEqual(#{<<"roomName">> => <<"thread">>,
                   <<"topicName">> => <<"messages">>,
                   <<"actorId">> => <<"actor-1">>,
                   <<"data">> => #{<<"text">> => <<"hi">>},
                   %% The scope id is filled now; it used to go out null.
                   <<"scope">> => #{<<"accessScopeId">> => <<"scope-1">>,
                                    <<"slug">> => <<"land-a">>,
                                    <<"name">> => <<"Land A">>}},
                 jsx:decode(Body, [return_maps])).

signed_trigger_sends_v2_test() ->
    {Headers, Body} = kraken_webhooks:trigger_request(
        #{<<"url">> => <<"http://x">>}, event(#{signing_secrets => [<<"whsec_test">>]})),
    {"nolag-signature", Sig} = lists:keyfind("nolag-signature", 1, Headers),
    {"nolag-webhook-id", "msg-1"} = lists:keyfind("nolag-webhook-id", 1, Headers),
    [<<"t=", T/binary>>, <<"v1=", Hex/binary>>] = binary:split(list_to_binary(Sig), <<",">>),
    Expected = binary:encode_hex(crypto:mac(hmac, sha256, <<"whsec_test">>,
                                            <<T/binary, ".", Body/binary>>), lowercase),
    ?assertEqual(Expected, Hex),
    Decoded = jsx:decode(Body, [return_maps]),
    ?assertMatch(#{<<"id">> := <<"msg-1">>,
                   <<"type">> := <<"message.published">>,
                   <<"projectId">> := <<"proj-1">>,
                   <<"appId">> := <<"app-1">>,
                   <<"roomId">> := <<"room-1">>,
                   <<"scopeId">> := <<"scope-1">>,
                   <<"topic">> := <<"messages">>,
                   <<"filter">> := null,
                   <<"sender">> := #{<<"type">> := <<"actor">>, <<"id">> := <<"actor-1">>,
                                     <<"actorType">> := <<"user">>},
                   <<"data">> := #{<<"text">> := <<"hi">>}},
                 Decoded).

backoff_grows_with_jitter_test() ->
    [?assert(D >= 800 andalso D =< 1200) || D <- [kraken_webhooks:backoff_ms(1, 1000) || _ <- lists:seq(1, 20)]],
    [?assert(D >= 3200 andalso D =< 4800) || D <- [kraken_webhooks:backoff_ms(2, 1000) || _ <- lists:seq(1, 20)]],
    ?assertEqual(0, kraken_webhooks:backoff_ms(1, 0)).
