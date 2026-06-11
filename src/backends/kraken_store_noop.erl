%%%-------------------------------------------------------------------
%% @doc No-op store backend: nothing is recorded, replay returns empty.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_store_noop).
-behaviour(kraken_store).

-export([
    init/0, terminate/0, is_enabled/0,
    log_message/1, log_delivery/1, mark_delivered/3, log_event/1,
    ack_delivery/2, batch_ack_deliveries/2,
    get_replay_messages/1, get_undelivered_count/2
]).

init() -> ok.
terminate() -> ok.
is_enabled() -> false.

log_message(_Doc) -> {ok, <<"noop">>}.
log_delivery(_Doc) -> ok.
mark_delivered(_MessageId, _ActorId, _Timestamp) -> ok.
log_event(_Doc) -> ok.
ack_delivery(_MessageId, _ActorId) -> ok.
batch_ack_deliveries(_ActorId, _MessageIds) -> ok.
get_replay_messages(_Options) -> {ok, [], 0}.
get_undelivered_count(_ActorId, _AppId) -> {ok, 0}.
