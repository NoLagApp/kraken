%%%-------------------------------------------------------------------
%% @doc No-op wake backend (default): no wake delivery. Standalone/syn
%% deployments cannot queue for an offline actor, so wake is a no-op.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_wake_noop).
-behaviour(kraken_wake).

-export([fire/1]).

fire(_WakeRequest) -> ok.
