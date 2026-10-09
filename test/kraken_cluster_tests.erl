%%%-------------------------------------------------------------------
%% @doc kraken_cluster tests: discovery configuration (env precedence and
%% the dns node basename), gossip announce signing, and the cookie never
%% being echoed into the log line.
%%
%% Pure functions only; the multi-node behaviour is exercised against real
%% containers, not here.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_cluster_tests).
-include_lib("eunit/include/eunit.hrl").

-define(VARS, ["CLUSTER_DNS_NAME", "CLUSTER_DNS_QUERY",
               "CLUSTER_NODE_BASENAME", "CLUSTER_GOSSIP_SECRET"]).

%% Save and clear the cluster env vars around each test, then restore them.
env_test_() ->
    {foreach,
     fun() ->
         Saved = [{V, os:getenv(V)} || V <- ?VARS],
         [os:unsetenv(V) || V <- ?VARS],
         Saved
     end,
     fun(Saved) ->
         [case Val of
              false -> os:unsetenv(V);
              _ -> os:putenv(V, Val)
          end || {V, Val} <- Saved]
     end,
     [fun dns_name_unset/0,
      fun dns_name_documented_name/0,
      fun dns_name_query_alias/0,
      fun dns_name_documented_name_wins/0,
      fun dns_name_empty_counts_as_unset/0,
      fun basename_defaults_to_own_node_name/0,
      fun basename_override/0,
      fun basename_empty_override_ignored/0,
      fun gossip_key_defaults_to_cookie/0,
      fun gossip_key_uses_secret/0]}.

dns_name_unset() ->
    ?assertEqual("", kraken_cluster:dns_name()).

dns_name_documented_name() ->
    os:putenv("CLUSTER_DNS_NAME", "kraken-headless.default.svc.cluster.local"),
    ?assertEqual("kraken-headless.default.svc.cluster.local", kraken_cluster:dns_name()).

dns_name_query_alias() ->
    os:putenv("CLUSTER_DNS_QUERY", "peers.internal"),
    ?assertEqual("peers.internal", kraken_cluster:dns_name()).

dns_name_documented_name_wins() ->
    os:putenv("CLUSTER_DNS_NAME", "new.name"),
    os:putenv("CLUSTER_DNS_QUERY", "old.name"),
    ?assertEqual("new.name", kraken_cluster:dns_name()).

%% The Dockerfile sets CLUSTER_DNS_NAME="" by default, which must not hide
%% a CLUSTER_DNS_QUERY set by an existing deployment.
dns_name_empty_counts_as_unset() ->
    os:putenv("CLUSTER_DNS_NAME", ""),
    os:putenv("CLUSTER_DNS_QUERY", "old.name"),
    ?assertEqual("old.name", kraken_cluster:dns_name()).

basename_defaults_to_own_node_name() ->
    ?assertEqual("kraken", kraken_cluster:node_basename(false, 'kraken@10.0.0.5')),
    ?assertEqual("kraken", kraken_cluster:node_basename(false, 'kraken@kraken1.cluster.local')),
    ?assertEqual("edge_7", kraken_cluster:node_basename(false, 'edge_7@192.168.1.20')),
    %% node() here is the test VM's own name.
    [Own | _] = string:split(atom_to_list(node()), "@"),
    ?assertEqual(Own, kraken_cluster:node_basename()).

basename_override() ->
    ?assertEqual("custom", kraken_cluster:node_basename("custom", 'kraken@10.0.0.5')),
    os:putenv("CLUSTER_NODE_BASENAME", "kraken_proxy"),
    ?assertEqual("kraken_proxy", kraken_cluster:node_basename()).

basename_empty_override_ignored() ->
    os:putenv("CLUSTER_NODE_BASENAME", ""),
    [Own | _] = string:split(atom_to_list(node()), "@"),
    ?assertEqual(Own, kraken_cluster:node_basename()).

gossip_key_defaults_to_cookie() ->
    ?assertEqual(atom_to_binary(erlang:get_cookie(), utf8), kraken_cluster:gossip_key()),
    os:putenv("CLUSTER_GOSSIP_SECRET", ""),
    ?assertEqual(atom_to_binary(erlang:get_cookie(), utf8), kraken_cluster:gossip_key()).

gossip_key_uses_secret() ->
    os:putenv("CLUSTER_GOSSIP_SECRET", "s3cret"),
    ?assertEqual(<<"s3cret">>, kraken_cluster:gossip_key()).

%%====================================================================
%% Gossip packets
%%====================================================================

gossip_roundtrip_test() ->
    Packet = kraken_cluster:gossip_packet(<<"k">>, 'kraken@10.0.0.5'),
    ?assertEqual({ok, 'kraken@10.0.0.5'}, kraken_cluster:verify_gossip_packet(<<"k">>, Packet)).

gossip_wrong_key_rejected_test() ->
    Packet = kraken_cluster:gossip_packet(<<"k">>, 'kraken@10.0.0.5'),
    ?assertEqual({error, bad_signature},
                 kraken_cluster:verify_gossip_packet(<<"other">>, Packet)).

%% The signature covers the node name, so a captured announce cannot be
%% replayed with a different name.
gossip_tampered_name_rejected_test() ->
    <<Mac:32/binary, _/binary>> = kraken_cluster:gossip_packet(<<"k">>, 'kraken@10.0.0.5'),
    Forged = <<Mac/binary, "evil@10.0.0.66">>,
    ?assertEqual({error, bad_signature}, kraken_cluster:verify_gossip_packet(<<"k">>, Forged)).

gossip_malformed_rejected_test() ->
    ?assertEqual({error, malformed}, kraken_cluster:verify_gossip_packet(<<"k">>, <<>>)),
    ?assertEqual({error, malformed}, kraken_cluster:verify_gossip_packet(<<"k">>, <<1, 2, 3>>)),
    ?assertEqual({error, malformed},
                 kraken_cluster:verify_gossip_packet(<<"k">>, binary:copy(<<0>>, 32))),
    %% Longer than any atom can be, even when correctly signed.
    Long = binary:copy(<<"a">>, 300),
    Signed = <<(crypto:mac(hmac, sha256, <<"k">>, Long))/binary, Long/binary>>,
    ?assertEqual({error, malformed}, kraken_cluster:verify_gossip_packet(<<"k">>, Signed)).

gossip_bad_node_name_rejected_test() ->
    Sign = fun(Name) -> <<(crypto:mac(hmac, sha256, <<"k">>, Name))/binary, Name/binary>> end,
    ?assertEqual({error, bad_node_name},
                 kraken_cluster:verify_gossip_packet(<<"k">>, Sign(<<"no_at_sign">>))),
    ?assertEqual({error, bad_node_name},
                 kraken_cluster:verify_gossip_packet(<<"k">>, Sign(<<"@host">>))),
    ?assertEqual({error, bad_node_name},
                 kraken_cluster:verify_gossip_packet(<<"k">>, Sign(<<"name@">>))),
    ?assertEqual({error, bad_node_name},
                 kraken_cluster:verify_gossip_packet(<<"k">>, Sign(<<"a@", 255, 254>>))).

%%====================================================================
%% Cookie logging
%%====================================================================

cookie_status_never_contains_cookie_test() ->
    Status = kraken_cluster:cookie_status('a-real-secret-cookie-9f3a'),
    ?assertEqual(nomatch, string:find(Status, "a-real-secret-cookie-9f3a")),
    ?assertEqual("custom value set", Status).

cookie_status_flags_example_cookies_test() ->
    [begin
         Status = kraken_cluster:cookie_status(C),
         ?assertMatch("WARNING" ++ _, Status),
         ?assertEqual(nomatch, string:find(Status, atom_to_list(C)))
     end || C <- [kraken_dev_cookie, kraken_cluster_cookie]].

cookie_status_not_distributed_test() ->
    ?assertMatch("none" ++ _, kraken_cluster:cookie_status(nocookie)).
