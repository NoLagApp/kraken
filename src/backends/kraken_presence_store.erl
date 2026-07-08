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
%% == Hot-path contract ==
%% Backends may do remote I/O with multi-second timeouts. The sync
%% callbacks below (upsert/offline/mark_waking/discover) must therefore
%% NEVER be called inline from a frame-processing path (publish, ack,
%% heartbeat): one inline call serializes every frame on the backend's
%% latency. This is not hypothetical — an inline per-publish discover
%% once throttled token streams to one message per store-timeout.
%% Frame paths use the *_async entry points, which detach via
%% kraken_detach before the backend is touched. The sync forms are for
%% request/response and lifecycle paths that need the result and can
%% tolerate backend latency.
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

%% Hot-path-safe (detached) entry points — see "Hot-path contract" above.
%% The async write forms are best-effort: a connect-advertise upsert and an
%% immediate disconnect-offline can in principle land out of order (window =
%% spawn scheduling, self-heals on the next transition). Presence records
%% feed discovery and the wake gate, both tolerant of a stale status.
-export([
    wake_offline_async/2,
    upsert_async/1,
    offline_async/1
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

%% Detached write-through for the presence-advertise frame: a remote
%% backend's write latency must not stall the advertising connection's
%% frame loop.
-spec upsert_async(Record :: map()) -> ok.
upsert_async(Record) ->
    kraken_detach:run(pp_upsert, fun() -> upsert(Record) end).

%% Detached soft-offline for terminate/3: a mass disconnect (deploy) must
%% not serialize N connection cleanups on N remote writes — a blocked
%% terminate holds the socket's resources until the backend timeout.
-spec offline_async(Key :: map()) -> ok.
offline_async(Key) ->
    kraken_detach:run(pp_offline, fun() -> offline(Key) end).

%% Publish-path wake gate: wake offline persistent subscribers in a room
%% so they reconnect and drain the message queued on their persistent
%% broker session. No-op on the OSS/syn build (discover returns []).
%% Owned here rather than in the handlers so the detach is structural:
%% callers cannot accidentally run the discover round trip inline. The
%% proxy's discover impl additionally caches to keep the detached load
%% off the store (one lookup per TTL, not per publish).
-spec wake_offline_async(RoomId :: binary() | undefined, AppId :: term()) -> ok.
wake_offline_async(undefined, _AppId) ->
    ok;
wake_offline_async(RoomId, AppId) ->
    kraken_detach:run(pp_wake_offline, fun() ->
        case discover(#{room_id => RoomId, app_id => AppId,
                        status => <<"offline">>}) of
            {ok, Actors} when is_list(Actors) ->
                lists:foreach(
                    fun(Actor) when is_map(Actor) ->
                            %% mark_waking debounces repeat wakes (status offline -> waking);
                            %% the wake backend extracts wake.url from the record and HMAC-POSTs.
                            catch mark_waking(Actor),
                            catch kraken_wake:fire(Actor);
                       (_) ->
                            ok
                    end, Actors);
            _ ->
                ok
        end
    end).
