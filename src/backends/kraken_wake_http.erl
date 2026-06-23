%%%-------------------------------------------------------------------
%% @doc HTTP wake backend — a runnable kraken_wake backend for local/dev/E2E.
%% Fires a fire-and-forget HMAC-signed POST to the actor's wake URL. Mirrors
%% the proxy's webhook_wake but lives in core so a standalone kraken (no proxy)
%% can wake. Secret from app env `wake_secret` (default "kraken_internal_secret").
%%
%% Enable with: wake_backend=kraken_wake_http
%% @end
%%%-------------------------------------------------------------------
-module(kraken_wake_http).
-behaviour(kraken_wake).

-export([fire/1]).

fire(Actor) when is_map(Actor) ->
    case maps:get(<<"wakeUrl">>, Actor, <<>>) of
        <<>> -> ok;
        Url -> spawn(fun() -> do_fire(Url, Actor) end), ok
    end;
fire(_) ->
    ok.

do_fire(Url, Actor) ->
    Body = jsx:encode(#{
        <<"appId">> => maps:get(<<"appId">>, Actor, <<>>),
        <<"roomId">> => maps:get(<<"roomId">>, Actor, <<>>),
        <<"actorTokenId">> => maps:get(<<"actorTokenId">>, Actor, <<>>),
        <<"reason">> => <<"dispatch">>,
        <<"ts">> => erlang:system_time(millisecond)
    }),
    Sig = "sha256=" ++ binary_to_list(bin_to_hex(crypto:mac(hmac, sha256, wake_secret(), Body))),
    Headers = [{"x-nolag-signature", Sig}],
    Request = {binary_to_list(Url), Headers, "application/json", Body},
    case httpc:request(post, Request, [{timeout, 10000}], []) of
        {ok, _} -> ok;
        {error, Reason} ->
            kraken_log:error("[WakeHttp] POST to ~s failed: ~p~n", [Url, Reason]),
            ok
    end.

wake_secret() ->
    case application:get_env(kraken, wake_secret, undefined) of
        S when is_binary(S) -> S;
        S when is_list(S) -> list_to_binary(S);
        _ -> <<"kraken_internal_secret">>
    end.

bin_to_hex(Bin) ->
    <<<<(hex(N div 16)), (hex(N rem 16))>> || <<N>> <= Bin>>.

hex(N) when N < 10 -> $0 + N;
hex(N) -> $a + (N - 10).
