%%%-------------------------------------------------------------------
%% @doc MQTT ingress tests: the ranch entry point, how MQTT topic filters map
%% to broker topics and back, and the payload rule between MQTT and
%% WebSocket clients (see docs/PROTOCOL.md, "MQTT ingress").
%% @end
%%%-------------------------------------------------------------------
-module(kraken_mqtt_handler_tests).
-include_lib("eunit/include/eunit.hrl").

-define(ME, <<"mqtt-me">>).

%% The rule from examples/auth.json
demo_rule() ->
    #{
        <<"pattern">> => <<"demo/general/#">>,
        <<"topic">> => <<"demo-general">>,
        <<"permission">> => <<"pubSub">>,
        <<"room_id">> => <<"room-general">>,
        <<"app_id">> => <<"demo-app">>
    }.

%% A control-plane style rule: exact pattern mapped to an internal topic
exact_rule() ->
    #{
        <<"pattern">> => <<"app/room/messages">>,
        <<"topic">> => <<"room-uuid-1/messages">>,
        <<"permission">> => <<"pubSub">>,
        <<"room_id">> => <<"room-uuid-1">>,
        <<"app_id">> => <<"app-uuid-1">>
    }.

pack(Term) -> kraken_msgpack:pack(Term).

%%====================================================================
%% Ranch entry point
%%====================================================================

%% cowboy 2.10 pulls ranch 1.8, which calls Protocol:start_link/4. With only
%% start_link/3 exported every MQTT connection crashed with undef.
start_link_matches_ranch_protocol_test() ->
    {module, _} = code:ensure_loaded(kraken_mqtt_handler),
    Callbacks = ranch_protocol:behaviour_info(callbacks),
    ?assert(lists:member({start_link, 4}, Callbacks)),
    [?assert(erlang:function_exported(kraken_mqtt_handler, F, A)) || {F, A} <- Callbacks],
    %% ranch 2.x arity stays available
    ?assert(erlang:function_exported(kraken_mqtt_handler, start_link, 3)).

%%====================================================================
%% Subscription topics
%%====================================================================

concrete_filter_resolves_like_publish_test() ->
    Rules = [demo_rule()],
    {wildcard, PublishTopic, _, _} = kraken_topics:resolve(<<"demo/general/chat">>, Rules),
    ?assertEqual(<<"demo-app/demo/general/chat">>, PublishTopic),
    ?assertEqual(PublishTopic, kraken_mqtt_handler:subscription_topic(<<"demo/general/chat">>, Rules)).

exact_filter_uses_internal_topic_test() ->
    ?assertEqual(<<"room-uuid-1/messages">>,
                 kraken_mqtt_handler:subscription_topic(<<"app/room/messages">>, [exact_rule()])).

%% A filter equal to the rule's own pattern must still match the topics
%% publishers use, not the rule's internal topic (`demo-general').
wildcard_filter_matches_publish_topics_test() ->
    Rules = [demo_rule()],
    Hash = kraken_mqtt_handler:subscription_topic(<<"demo/general/#">>, Rules),
    Plus = kraken_mqtt_handler:subscription_topic(<<"demo/general/+">>, Rules),
    ?assertEqual(<<"demo-app/demo/general/#">>, Hash),
    ?assertEqual(<<"demo-app/demo/general/+">>, Plus),
    {wildcard, PublishTopic, _, _} = kraken_topics:resolve(<<"demo/general/chat">>, Rules),
    ?assert(kraken_acl:matches_pattern(PublishTopic, Hash)),
    ?assert(kraken_acl:matches_pattern(PublishTopic, Plus)).

%%====================================================================
%% Display topics
%%====================================================================

display_exact_subscription_test() ->
    Subs = #{<<"app/room/messages">> => <<"room-uuid-1/messages">>},
    ?assertEqual(<<"app/room/messages">>,
                 kraken_mqtt_handler:display_topic(<<"room-uuid-1/messages">>, Subs)).

display_wildcard_subscription_test() ->
    Subs = #{<<"demo/general/#">> => <<"demo-app/demo/general/#">>},
    ?assertEqual(<<"demo/general/chat">>,
                 kraken_mqtt_handler:display_topic(<<"demo-app/demo/general/chat">>, Subs)),
    %% a filtered WebSocket publish lands one level down
    ?assertEqual(<<"demo/general/chat/vip">>,
                 kraken_mqtt_handler:display_topic(<<"demo-app/demo/general/chat/vip">>, Subs)).

display_single_level_wildcard_test() ->
    Subs = #{<<"demo/general/+">> => <<"demo-app/demo/general/+">>},
    ?assertEqual(<<"demo/general/chat">>,
                 kraken_mqtt_handler:display_topic(<<"demo-app/demo/general/chat">>, Subs)),
    ?assertEqual(undefined,
                 kraken_mqtt_handler:display_topic(<<"demo-app/demo/general/chat/vip">>, Subs)).

display_prefers_exact_subscription_test() ->
    Subs = #{<<"demo/general/#">> => <<"demo-app/demo/general/#">>,
             <<"demo/general/chat">> => <<"demo-app/demo/general/chat">>},
    ?assertEqual(<<"demo/general/chat">>,
                 kraken_mqtt_handler:display_topic(<<"demo-app/demo/general/chat">>, Subs)).

display_unsubscribed_topic_test() ->
    ?assertEqual(undefined, kraken_mqtt_handler:display_topic(<<"demo-app/demo/general/chat">>, #{})),
    Subs = #{<<"demo/general/chat">> => <<"demo-app/demo/general/chat">>},
    ?assertEqual(undefined, kraken_mqtt_handler:display_topic(<<"demo-app/demo/general/other">>, Subs)).

%%====================================================================
%% Delivery payloads
%%====================================================================

string_data_is_sent_raw_test() ->
    ?assertEqual({ok, <<"hello">>}, kraken_mqtt_handler:delivery_payload(pack(<<"hello">>), ?ME)).

binary_data_is_sent_raw_test() ->
    Bytes = <<255, 0, 1, 254>>,
    ?assertEqual({ok, Bytes}, kraken_mqtt_handler:delivery_payload(pack(Bytes), ?ME)).

structured_data_is_sent_as_msgpack_test() ->
    Data = #{<<"temp">> => 21.5, <<"tags">> => [<<"a">>, 1]},
    {ok, Bytes} = kraken_mqtt_handler:delivery_payload(pack(Data), ?ME),
    ?assertEqual({ok, Data}, msgpack:unpack(Bytes, [{unpack_str, as_binary}])),
    ?assertEqual({ok, pack(42)}, kraken_mqtt_handler:delivery_payload(pack(42), ?ME)).

no_echo_envelope_is_unwrapped_test() ->
    Envelope = #{<<"data">> => <<"hi">>, <<"_sender">> => <<"ws-other">>},
    ?assertEqual({ok, <<"hi">>}, kraken_mqtt_handler:delivery_payload(pack(Envelope), ?ME)).

own_publish_is_dropped_test() ->
    Envelope = #{<<"data">> => <<"hi">>, <<"_sender">> => ?ME},
    ?assertEqual(drop, kraken_mqtt_handler:delivery_payload(pack(Envelope), ?ME)).

msg_id_wrapper_is_removed_test() ->
    Tracked = #{<<"_msgId">> => <<"m-1">>, <<"_data">> => #{<<"n">> => 1}},
    ?assertEqual({ok, pack(#{<<"n">> => 1})},
                 kraken_mqtt_handler:delivery_payload(pack(Tracked), ?ME)),
    Enveloped = #{<<"data">> => Tracked, <<"_sender">> => <<"ws-other">>},
    ?assertEqual({ok, pack(#{<<"n">> => 1})},
                 kraken_mqtt_handler:delivery_payload(pack(Enveloped), ?ME)).

non_msgpack_payload_is_forwarded_test() ->
    %% 0xC1 is never used by MessagePack
    ?assertEqual({ok, <<16#C1, 1, 2>>}, kraken_mqtt_handler:delivery_payload(<<16#C1, 1, 2>>, ?ME)).

%%====================================================================
%% kraken_msgpack
%%====================================================================

msgpack_same_bytes_for_valid_terms_test() ->
    Opts = [{pack_str, from_binary}],
    Terms = [<<"plain">>, <<"héllo"/utf8>>, 7, -3, 1.5, null, true,
             [1, <<"x">>, [2]], #{<<"k">> => #{<<"n">> => [1, 2, 3]}},
             lists:seq(1, 20), maps:from_list([{integer_to_binary(N), N} || N <- lists:seq(1, 20)])],
    [?assertEqual(msgpack:pack(T, Opts), kraken_msgpack:pack(T)) || T <- Terms].

msgpack_non_utf8_binary_packs_as_bin_test() ->
    Bytes = <<255, 0, 1>>,
    %% msgpack-erlang itself cannot pack it as a string
    ?assertError(badarg, msgpack:pack(Bytes, [{pack_str, from_binary}])),
    ?assertEqual(<<16#C4, 3, 255, 0, 1>>, kraken_msgpack:pack(Bytes)),
    Envelope = #{<<"data">> => Bytes, <<"_sender">> => <<"mqtt-x">>},
    ?assertEqual({ok, Envelope},
                 msgpack:unpack(kraken_msgpack:pack(Envelope), [{unpack_str, as_binary}])),
    Big = binary:copy(<<255>>, 70000),
    ?assertEqual({ok, Big}, msgpack:unpack(kraken_msgpack:pack(Big), [{unpack_str, as_binary}])),
    %% map16 / array16 headers on the fallback path
    LargeMap = maps:from_list([{integer_to_binary(N), N} || N <- lists:seq(1, 20)]),
    Nested = #{<<"bytes">> => Bytes, <<"map">> => LargeMap,
               <<"list">> => [Bytes | lists:seq(1, 20)], <<"text">> => <<"héllo"/utf8>>},
    ?assertEqual({ok, Nested},
                 msgpack:unpack(kraken_msgpack:pack(Nested), [{unpack_str, as_binary}])).

%%====================================================================
%% Through the syn broker backend
%%====================================================================

syn_setup() ->
    application:ensure_all_started(syn),
    catch syn:add_node_to_scopes([kraken_topics]),
    ok = kraken_broker_syn:start().

flush_broker_messages() ->
    receive
        {mqtt_publish, _} -> flush_broker_messages();
        {store_topic_mapping, _, _} -> flush_broker_messages()
    after 0 -> ok
    end.

%% A wildcard subscriber learns the published topic (source_topic), and an
%% MQTT client's non-UTF-8 payload survives the broker byte for byte.
syn_wildcard_delivery_test() ->
    syn_setup(),
    Session = #{pid => self()},
    Filter = <<"demo/general/#">>,
    BrokerTopic = kraken_mqtt_handler:subscription_topic(Filter, [demo_rule()]),
    ok = kraken_broker_syn:subscribe(Session, BrokerTopic, Filter, self(), 0),
    Bytes = <<255, 0, 1>>,
    ok = kraken_broker_syn:publish(Session, <<"demo-app/demo/general/chat">>, Bytes, <<"mqtt-sender">>, 0, false),
    receive
        {mqtt_publish, #{topic := BrokerTopic, payload := Packed} = Msg} ->
            Published = maps:get(source_topic, Msg),
            ?assertEqual(<<"demo-app/demo/general/chat">>, Published),
            ?assertEqual(<<"demo/general/chat">>,
                         kraken_mqtt_handler:display_topic(Published, #{Filter => BrokerTopic})),
            ?assertEqual({ok, Bytes}, kraken_mqtt_handler:delivery_payload(Packed, ?ME))
    after 1000 ->
        erlang:error(no_delivery)
    end,
    ok = kraken_broker_syn:unsubscribe(Session, BrokerTopic),
    flush_broker_messages(),
    ok = kraken_broker_syn:publish(Session, <<"demo-app/demo/general/chat">>, <<"again">>, undefined, 0, false),
    receive
        {mqtt_publish, _} = Late -> erlang:error({delivered_after_unsubscribe, Late})
    after 200 -> ok
    end.
