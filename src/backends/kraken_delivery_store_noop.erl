%%%-------------------------------------------------------------------
%% @doc No-op durable delivery store (default): no durable claim-based
%% replay. is_enabled/0 => false, so the load-balanced offline→replay gate
%% never fires and behaviour is identical to pre-durable-delivery kraken
%% (normal subscriptions keep using EMQX session-queue replay). claim/1
%% returns `won' so a single-consumer/dev path still drains if a backend
%% is mis-wired.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_delivery_store_noop).
-behaviour(kraken_delivery_store).

-export([init/0, is_enabled/0, pending/1, claim/1, cursor_get/1, cursor_set/2, ack/1]).

init() -> ok.
is_enabled() -> false.
pending(_Query) -> {ok, []}.
claim(_Req) -> {ok, won}.
cursor_get(_Key) -> {ok, 0}.
cursor_set(_Key, _Pos) -> ok.
ack(_Req) -> ok.
