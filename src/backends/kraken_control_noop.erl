%%%-------------------------------------------------------------------
%% @doc No-op control backend (default): no external control plane.
%% Usage is never reported, no project is ever quota-blocked.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_control_noop).
-behaviour(kraken_control).

-export([report_usage/1, report_subscription_change/1, report_webhook_failure/1, ensure_room/4]).

report_usage(_Entries) -> ok.
report_subscription_change(_Changes) -> ok.
report_webhook_failure(_Failure) -> ok.
ensure_room(_ProjectId, _AppId, _RoomSlug, _ActorTokenId) -> {error, not_supported}.
