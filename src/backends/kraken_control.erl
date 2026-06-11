%%%-------------------------------------------------------------------
%% @doc Control-plane behaviour + dispatcher.
%%
%% Hot-path integration points for an external control plane (billing,
%% quota enforcement, subscription tracking). Batching, caching and
%% scheduling live in kraken core (kraken_usage / kraken_subscriptions);
%% backends are delivery-only and therefore tiny.
%%
%% Built-ins: kraken_control_noop (default - standalone deployments do
%% nothing) and kraken_control_http (generic webhook delivery; doubles
%% as the contract a hosted control plane implements).
%%
%% Concerns deliberately NOT here (sidecar territory for embedding
%% applications, via kraken:stats/0): heartbeats, version polling,
%% upgrade orchestration.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_control).

%% Behaviour
-callback report_usage(Entries :: [map()]) ->
    ok | {ok, BlockedProjectIds :: [binary()]} | {error, term()}.
-callback report_subscription_change(Changes :: [map()]) ->
    ok | {error, term()}.
-callback report_webhook_failure(Failure :: map()) ->
    ok.

-export([
    report_usage/1,
    report_subscription_change/1,
    report_webhook_failure/1
]).

backend() -> kraken:backend(control).

report_usage(Entries) ->
    (backend()):report_usage(Entries).

report_subscription_change(Changes) ->
    (backend()):report_subscription_change(Changes).

report_webhook_failure(Failure) ->
    (backend()):report_webhook_failure(Failure).
