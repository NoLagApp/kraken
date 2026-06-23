%%%-------------------------------------------------------------------
%% @doc Wake behaviour + dispatcher.
%%
%% Fires an out-of-band, HMAC-signed webhook to bring a persistent but
%% offline actor back online when a message/task is dispatched to it.
%% The actual task is queued on the actor's persistent broker session
%% (EMQX) and flushed on reconnect; this slot only delivers the wake
%% signal (see kraken-proxy/docs/PERSISTENT_PRESENCE.md "Wake webhook
%% contract").
%%
%% Built-in default: kraken_wake_noop (standalone/syn deployments cannot
%% queue for an offline actor, so there is nothing to wake to).
%%
%% A WakeRequest is a map carrying at least:
%%   app_id, room_id, actor_token_id, wake (the registered #{url,
%%   timeout_ms}), capability, wake_id, reason.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_wake).

%% Behaviour
-callback fire(WakeRequest :: map()) -> ok | {error, term()}.

-export([fire/1]).

backend() -> kraken:backend(wake).

fire(WakeRequest) ->
    (backend()):fire(WakeRequest).
