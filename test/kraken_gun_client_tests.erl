%%%-------------------------------------------------------------------
%% @doc Base-path handling for the HTTP client.
%%
%% Backends pass an endpoint path alone (<<"/validate">>) and the client
%% prefixes whatever path the configured URL carried. Without that, a
%% control plane mounted under a prefix receives requests at the root and
%% answers 404, which surfaces as an authentication failure rather than as
%% a configuration error.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_gun_client_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% normalise_base_path/1
%%====================================================================

no_path_is_empty_test() ->
    ?assertEqual(<<>>, kraken_gun_client:normalise_base_path(<<>>)),
    ?assertEqual(<<>>, kraken_gun_client:normalise_base_path(<<"/">>)),
    ?assertEqual(<<>>, kraken_gun_client:normalise_base_path("")).

trailing_slash_is_dropped_test() ->
    ?assertEqual(<<"/v1/internal/actors">>,
                 kraken_gun_client:normalise_base_path(<<"/v1/internal/actors/">>)),
    ?assertEqual(<<"/v1/internal/actors">>,
                 kraken_gun_client:normalise_base_path(<<"/v1/internal/actors">>)).

leading_slash_is_added_test() ->
    ?assertEqual(<<"/core">>, kraken_gun_client:normalise_base_path(<<"core">>)).

lists_are_accepted_test() ->
    ?assertEqual(<<"/v1/internal/actors">>,
                 kraken_gun_client:normalise_base_path("/v1/internal/actors/")).

%%====================================================================
%% What a parsed URL yields
%%
%% uri_string:parse/1 is what init/1 uses, so these pin the two shapes an
%% operator actually configures.
%%====================================================================

origin_only_url_test() ->
    Parsed = uri_string:parse(<<"http://core:3000">>),
    ?assertEqual(<<>>,
                 kraken_gun_client:normalise_base_path(maps:get(path, Parsed, <<>>))).

url_with_prefix_test() ->
    Parsed = uri_string:parse(<<"http://core:3000/v1/internal/actors">>),
    ?assertEqual(<<"/v1/internal/actors">>,
                 kraken_gun_client:normalise_base_path(maps:get(path, Parsed, <<>>))).

%%====================================================================
%% Concatenation
%%
%% The property that matters: base ++ endpoint is the path the control
%% plane actually serves.
%%====================================================================

concatenation_test() ->
    Base = kraken_gun_client:normalise_base_path(<<"/v1/internal/actors/">>),
    ?assertEqual(<<"/v1/internal/actors/validate">>,
                 <<Base/binary, <<"/validate">>/binary>>),
    ?assertEqual(<<"/v1/internal/actors/check-room-access">>,
                 <<Base/binary, <<"/check-room-access">>/binary>>).

bare_origin_leaves_endpoint_untouched_test() ->
    Base = kraken_gun_client:normalise_base_path(<<>>),
    ?assertEqual(<<"/validate">>, <<Base/binary, <<"/validate">>/binary>>).
