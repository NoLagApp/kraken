%%%-------------------------------------------------------------------
%% @doc The auth dispatcher's token cache and the auth_result shape.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_auth_cache_tests).

-include_lib("eunit/include/eunit.hrl").

scope_fields_survive_test() ->
    Auth = kraken_auth:build_auth_data(#{
        <<"actor_token_id">> => <<"a">>,
        <<"scope_slug">> => <<"land-a">>,
        <<"scope_id">> => <<"scope-uuid">>,
        <<"scope_name">> => <<"Land A">>
    }),
    ?assertEqual(<<"land-a">>, maps:get(scope_slug, Auth)),
    ?assertEqual(<<"scope-uuid">>, maps:get(scope_id, Auth)),
    ?assertEqual(<<"Land A">>, maps:get(scope_name, Auth)).

unscoped_actor_has_no_scope_test() ->
    Auth = kraken_auth:build_auth_data(#{<<"actor_token_id">> => <<"a">>, <<"scope_slug">> => null}),
    ?assertEqual(undefined, maps:get(scope_slug, Auth)),
    ?assertEqual(undefined, maps:get(scope_id, Auth)),
    ?assertEqual(undefined, maps:get(scope_name, Auth)).

cache_test_() ->
    Path = filename:join(["/tmp", "kraken_auth_cache_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".json"]),
    {setup,
     fun() ->
         ok = file:write_file(Path, jsx:encode(#{<<"tokens">> => #{
             <<"tok-a">> => #{<<"actorTokenId">> => <<"actor-a">>, <<"allowedTopics">> => []},
             <<"tok-b">> => #{<<"actorTokenId">> => <<"actor-b">>, <<"allowedTopics">> => []}
         }})),
         application:set_env(kraken, auth_backend, kraken_auth_static),
         application:set_env(kraken, auth_file, Path),
         application:set_env(kraken, auth_allow_all, false)
     end,
     fun(_) ->
         file:delete(Path),
         application:unset_env(kraken, auth_file)
     end,
     [fun each_token_gets_its_own_actor/0,
      fun cache_outlives_the_process_that_filled_it/0]}.

each_token_gets_its_own_actor() ->
    {ok, A} = kraken_auth:validate_token(<<"tok-a">>),
    {ok, B} = kraken_auth:validate_token(<<"tok-b">>),
    ?assertEqual(<<"actor-a">>, maps:get(actor_token_id, A)),
    ?assertEqual(<<"actor-b">>, maps:get(actor_token_id, B)),
    %% Keyed on the token's SHA-256, never a short hash two tokens can share.
    Keys = [K || {K, _, _} <- ets:tab2list(kraken_auth_token_cache)],
    ?assert(lists:member(crypto:hash(sha256, <<"tok-a">>), Keys)),
    ?assert(lists:all(fun(K) -> is_binary(K) andalso byte_size(K) =:= 32 end, Keys)).

cache_outlives_the_process_that_filled_it() ->
    %% The supervisor owns the table in a running node; here this test
    %% process does. A short-lived caller (an HTTP request) must not take it
    %% down with it when it exits.
    kraken_auth:ensure_cache(),
    Parent = self(),
    Pid = spawn(fun() -> Parent ! {self(), kraken_auth:validate_token(<<"tok-a">>)} end),
    receive {Pid, {ok, _}} -> ok after 2000 -> error(timeout) end,
    timer:sleep(20),
    ?assertNotEqual(undefined, ets:whereis(kraken_auth_token_cache)),
    ?assertMatch({ok, _}, kraken_auth:validate_token(<<"tok-b">>)).
