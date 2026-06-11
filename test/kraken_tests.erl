-module(kraken_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Backend resolution
%%====================================================================

backend_resolution_test() ->
    application:set_env(kraken, auth_backend, static),
    ?assertEqual(kraken_auth_static, kraken:backend(auth)),
    application:set_env(kraken, broker_backend, syn),
    ?assertEqual(kraken_broker_syn, kraken:backend(broker)),
    application:set_env(kraken, broker_backend, mqtt),
    ?assertEqual(kraken_broker_mqtt, kraken:backend(broker)),
    %% custom module atoms pass through
    application:set_env(kraken, store_backend, my_custom_store),
    ?assertEqual(my_custom_store, kraken:backend(store)),
    application:set_env(kraken, store_backend, ets),
    application:set_env(kraken, broker_backend, syn),
    ok.

%%====================================================================
%% ACL pattern matching
%%====================================================================

acl_test_() ->
    Topics = [
        #{<<"pattern">> => <<"chat/general/#">>, <<"permission">> => <<"pubSub">>},
        #{<<"pattern">> => <<"telemetry/+/data">>, <<"permission">> => <<"publish">>},
        #{<<"pattern">> => <<"alerts/all">>, <<"permission">> => <<"subscribe">>}
    ],
    [
        ?_assert(kraken_acl:can_publish(<<"chat/general/messages">>, Topics)),
        ?_assert(kraken_acl:can_subscribe(<<"chat/general/messages">>, Topics)),
        ?_assert(kraken_acl:can_publish(<<"telemetry/device1/data">>, Topics)),
        ?_assertNot(kraken_acl:can_subscribe(<<"telemetry/device1/data">>, Topics)),
        ?_assertNot(kraken_acl:can_publish(<<"alerts/all">>, Topics)),
        ?_assert(kraken_acl:can_subscribe(<<"alerts/all">>, Topics)),
        ?_assertNot(kraken_acl:can_publish(<<"other/topic">>, Topics))
    ].

%%====================================================================
%% ETS store: log + replay + ack
%%====================================================================

store_ets_test() ->
    application:set_env(kraken, store_ttl_seconds, 3600),
    application:set_env(kraken, store_max_messages, 100),
    ok = kraken_store_ets:init(),
    Now = erlang:system_time(millisecond),
    Doc = #{
        message_id => <<"m1">>, organization_id => <<"o">>, project_id => <<"p">>,
        app_id => <<"a">>, room_id => <<"r">>, topic => <<"r/t">>,
        topic_name => <<"t">>, pattern => <<"app/room/t">>,
        sender_actor_id => <<"alice">>,
        payload => msgpack:pack(#{<<"text">> => <<"hi">>}, [{pack_str, from_binary}]),
        payload_size => 10, timestamp => Now
    },
    {ok, <<"m1">>} = kraken_store_ets:log_message(Doc),
    ok = kraken_store_ets:log_delivery(#{message_id => <<"m1">>, actor_id => <<"bob">>,
                                         topic => <<"r/t">>, delivered_at => Now, status => pending}),

    %% pending delivery -> replayed
    {ok, Msgs, 1} = kraken_store_ets:get_replay_messages(#{actor_id => <<"bob">>, limit => 10}),
    [#{message_id := <<"m1">>, payload := #{<<"text">> := <<"hi">>}}] = Msgs,
    {ok, 1} = kraken_store_ets:get_undelivered_count(<<"bob">>, <<"a">>),

    %% acked -> no longer replayed
    ok = kraken_store_ets:ack_delivery(<<"m1">>, <<"bob">>),
    {ok, [], 0} = kraken_store_ets:get_replay_messages(#{actor_id => <<"bob">>, limit => 10}),
    {ok, 0} = kraken_store_ets:get_undelivered_count(<<"bob">>, <<"a">>),
    ok.

%%====================================================================
%% Static auth backend
%%====================================================================

auth_static_test() ->
    Path = "/tmp/kraken_test_auth.json",
    Json = jsx:encode(#{<<"tokens">> => #{
        <<"tok-1">> => #{
            <<"actorTokenId">> => <<"actor-1">>,
            <<"projectId">> => <<"proj-1">>,
            <<"allowedTopics">> => [#{
                <<"pattern">> => <<"demo/#">>,
                <<"permission">> => <<"pubSub">>
            }]
        }
    }}),
    ok = file:write_file(Path, Json),
    application:set_env(kraken, auth_file, Path),
    application:set_env(kraken, auth_allow_all, false),

    {ok, Auth} = kraken_auth_static:validate_token(<<"tok-1">>),
    ?assertEqual(<<"actor-1">>, maps:get(actor_token_id, Auth)),
    ?assertEqual(<<"proj-1">>, maps:get(project_id, Auth)),
    [Topic] = maps:get(allowed_topics, Auth),
    ?assertEqual(<<"demo/#">>, maps:get(<<"pattern">>, Topic)),

    {error, <<"access_denied">>} = kraken_auth_static:validate_token(<<"nope">>),

    %% revalidation by actor id
    {ok, _} = kraken_auth_static:revalidate_token(<<"actor-1">>),
    {error, <<"token_revoked">>} = kraken_auth_static:revalidate_token(<<"ghost">>),
    ok.

%%====================================================================
%% Auth helpers
%%====================================================================

auth_helpers_test() ->
    ?assertEqual(unlimited, kraken_auth:parse_max_connections(null)),
    ?assertEqual(5, kraken_auth:parse_max_connections(5)),
    application:set_env(kraken, max_message_size, 921600),
    ?assertEqual(921600, kraken_auth:parse_max_message_size(undefined)),
    ?assertEqual(1024, kraken_auth:parse_max_message_size(1024)),
    ok.
