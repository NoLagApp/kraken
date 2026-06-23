%%%-------------------------------------------------------------------
%% @doc Durable delivery store behaviour + dispatcher.
%%
%% Claim-based replay for LOAD-BALANCED workers that have scaled to zero.
%% A task published to a shared subscription `$share/<group>/<topic>'
%% whose group is all-offline is dropped by EMQX (shared subscriptions do
%% not buffer for an offline group). With this slot enabled, on reconnect
%% the woken worker replays the messages it missed from the durable
%% message store (the existing `messages' collection — NO duplicate copy)
%% and atomically CLAIMS each one, so exactly one group member processes
%% it. See kraken-proxy/docs and the durable-delivery plan.
%%
%% Like the other plugin slots, backends are storage/claim only; the
%% lifecycle (when to replay, dedup against live, advance cursor) lives in
%% kraken core (kraken_replay + kraken_ws_handler). Built-in default:
%% kraken_delivery_store_noop (is_enabled/0 => false), so non-load-balanced
%% and OSS deployments behave exactly as before — normal subscriptions
%% keep using EMQX session-queue replay, untouched.
%%
%% Map conventions (keys are atoms):
%%   pending(Query)  Query = #{app_id, room_id, group_id, cursor, limit}
%%                   returns [#{message_id, topic, pattern, payload, timestamp}]
%%   claim(Req)      Req   = #{app_id, group_id, message_id, actor_id}
%%   cursor_get/set  Key   = #{app_id, group_id}; Pos = monotonic int (ms)
%%   ack(Req)        Req   = #{app_id, group_id, message_id, actor_id}
%% @end
%%%-------------------------------------------------------------------
-module(kraken_delivery_store).

%% Behaviour
-callback init() -> ok | {error, term()}.
-callback is_enabled() -> boolean().
-callback pending(Query :: map()) -> {ok, [map()]} | {error, term()}.
-callback claim(Req :: map()) -> {ok, won | lost} | {error, term()}.
-callback cursor_get(Key :: map()) -> {ok, integer()} | {error, term()}.
-callback cursor_set(Key :: map(), Pos :: integer()) -> ok | {error, term()}.
-callback ack(Req :: map()) -> ok | {error, term()}.

-export([
    is_enabled/0,
    pending/1,
    claim/1,
    cursor_get/1,
    cursor_set/2,
    ack/1
]).

backend() -> kraken:backend(delivery_store).

%% Durable delivery is opt-in at the deployment level (durable_delivery env)
%% AND requires the backend to be enabled. Off => the load-balanced replay
%% gate never fires and behaviour is unchanged (normal subs use EMQX).
-spec is_enabled() -> boolean().
is_enabled() ->
    durable_on() andalso (backend()):is_enabled().

durable_on() ->
    case application:get_env(kraken, durable_delivery, false) of
        true -> true;
        "true" -> true;
        <<"true">> -> true;
        _ -> false
    end.

pending(Query) -> (backend()):pending(Query).
claim(Req) -> (backend()):claim(Req).
cursor_get(Key) -> (backend()):cursor_get(Key).
cursor_set(Key, Pos) -> (backend()):cursor_set(Key, Pos).
ack(Req) -> (backend()):ack(Req).
