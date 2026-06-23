%%%-------------------------------------------------------------------
%% @doc No-op presence store (default): no durable presence registry.
%% Presence stays ephemeral and socket-bound (syn only). discover/1
%% returns [] so the offline→wake gate never fires and behaviour is
%% identical to pre-Persistent-Presence kraken.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_presence_store_noop).
-behaviour(kraken_presence_store).

-export([upsert/1, offline/1, mark_waking/1, discover/1]).

upsert(_Record) -> ok.
offline(_Key) -> ok.
mark_waking(_Key) -> ok.
discover(_Query) -> {ok, []}.
