%%%-------------------------------------------------------------------
%% @doc Persistent-presence store behaviour + dispatcher.
%%
%% Durable presence records that survive socket disconnection, so an
%% actor that has scaled to zero stays discoverable and is woken on
%% dispatch (see docs/PROTOCOL.md "Persistent presence" and
%% kraken-proxy/docs/PERSISTENT_PRESENCE.md).
%%
%% Like the other plugin slots, backends are delivery/storage only;
%% the lifecycle decisions (when to write-through, soft-offline, wake)
%% live in kraken core. Built-in default: kraken_presence_store_noop
%% (standalone/syn deployments keep ephemeral, socket-bound presence —
%% discover/1 returns [] so the offline→wake gate never fires).
%%
%% A Record/Key/Query is a map carrying at least:
%%   app_id, room_id, actor_token_id
%% upsert/1 additionally carries: capabilities, advertisement, wake,
%%   node, advertisement_version.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_presence_store).

%% Behaviour
-callback upsert(Record :: map()) -> ok | {error, term()}.
-callback offline(Key :: map()) -> ok | {error, term()}.
-callback mark_waking(Key :: map()) -> ok | {error, term()}.
-callback discover(Query :: map()) -> {ok, [map()]} | {error, term()}.

-export([
    upsert/1,
    offline/1,
    mark_waking/1,
    discover/1
]).

backend() -> kraken:backend(presence_store).

upsert(Record) ->
    (backend()):upsert(Record).

offline(Key) ->
    (backend()):offline(Key).

mark_waking(Key) ->
    (backend()):mark_waking(Key).

discover(Query) ->
    (backend()):discover(Query).
