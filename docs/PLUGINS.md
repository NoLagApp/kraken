# Writing Kraken Backends

Kraken has four plugin slots, each an Erlang behaviour. Configure a slot with
a short name (built-in) or any module atom (your custom backend):

```erlang
%% sys.config (or AUTH_BACKEND / BROKER_BACKEND / STORE_BACKEND / CONTROL_BACKEND env)
{kraken, [
    {auth_backend, my_auth},        %% custom module implementing kraken_auth
    {broker_backend, syn},          %% built-in short name
    {store_backend, my_pg_store},
    {control_backend, noop}
]}
```

When embedding kraken as a rebar3 dependency, ship your backend modules in
your wrapper app and set the config — no kraken changes needed.

---

## kraken_auth — token validation

```erlang
-callback validate_token(AccessToken :: binary()) ->
    {ok, AuthData :: map()} | {error, Reason :: binary()}.
-callback revalidate_token(ActorTokenId :: binary()) ->
    {ok, AuthData :: map()} | {error, Reason :: binary()} | {retry, Reason :: binary()}.
```

`AuthData` (atom keys) — build it with `kraken_auth:build_auth_data/1` from a
binary-keyed attrs map:

| Key | Meaning |
|-----|---------|
| `actor_token_id`, `organization_id`, `project_id`, `actor_type` | identity |
| `apps` | list of app maps: `app_id`, `app_name`, `allowed_topics`, `active_subscriptions`, `allowed_lobbies`, `hydration_webhook`, `trigger_webhook` |
| `allowed_topics` | flattened ACL rules: `pattern` (MQTT-style, `+`/`#`), `topic` (internal broker topic), `permission` (`pubSub`/`publish`/`subscribe`), `room_id`, `room_slug` |
| `max_connections`, `max_message_size_bytes` | limits (`unlimited` / bytes) |
| `scope_slug` | multi-tenant scope injection (optional) |
| `persistent_session`, `session_expiry_seconds` | broker session hints |

The dispatcher (`kraken_auth`) owns a 30s token cache; revalidation runs every
10 minutes per connection — `{error, Reason}` disconnects the actor,
`{retry, _}` tries again next cycle.

**The HTTP contract** (`kraken_auth_http`) for external control planes:

```
POST {AUTH_HTTP_URL}/validate     {"accessToken": "..."}
  -> {"result": "allow", "client_attrs": { ...attrs... }} | {"result": "deny"}
POST {AUTH_HTTP_URL}/revalidate   {"actorTokenId": "..."}
  -> {"valid": true, ...attrs...} | {"valid": false, "disconnect_reason": "..."}
```
Authorization: `Bearer {BACKEND_SECRET}`.

## kraken_broker — fan-out

```erlang
-callback start() -> ok.
-callback connect() -> {ok, Session} | {error, term()}.
-callback connect(AuthData) -> {ok, Session} | {error, term()}.
-callback connect(AuthData, PersistentSession, SessionExpirySeconds) ->
    {ok, Session, ClientId} | {error, term()}.
-callback subscribe(Session, MqttTopic, DisplayTopic, WsPid, QoS) -> ok.
-callback unsubscribe(Session, Topic) -> ok.
-callback publish(Session, Topic, Data, Sender | undefined, QoS, Retain) -> ok.
-callback disconnect(Session) -> ok.
-callback format_shared_subscription(BaseTopic, Group) -> binary().
-callback supports_load_balancing() -> boolean().
-callback capabilities() -> map().   %% #{retained, shared_subscriptions, multi_region}
```

Delivery contract: subscribed `WsPid`s must receive
`{mqtt_publish, #{topic := Topic, payload := PackedPayload}}` where the
payload is msgpack (`[{pack_str, from_binary}]`); when `Sender` is set, wrap
`#{<<"data">> => Data, <<"_sender">> => Sender}` (echo suppression).
Shared subscriptions (`$share/Group/Topic`) deliver to exactly one member.

## kraken_store — history + replay

```erlang
-callback init() -> ok | {error, term()}.
-callback is_enabled() -> boolean().
-callback log_message(Doc) -> ok | {error, term()}.
-callback log_delivery(Doc) -> ok | {error, term()}.
-callback mark_delivered(MessageId, ActorId, Timestamp) -> ok.
-callback log_event(Doc) -> ok.
-callback ack_delivery(MessageId, ActorId) -> ok | {error, term()}.
-callback batch_ack_deliveries(ActorId, MessageIds) -> ok | {error, term()}.
-callback get_replay_messages(Options) -> {ok, [Msg], Count} | {error, term()}.
-callback get_undelivered_count(ActorId, AppId) -> {ok, Count} | {error, term()}.
-callback terminate() -> ok.
```

Message docs carry `message_id`, org/project/app/room ids, `topic` (internal),
`topic_name` (bare), `pattern` (human), `sender_actor_id`,
`payload` (raw msgpack binary), `payload_size`, `timestamp` (ms).
Replay messages must return `payload` as the **unpacked** term plus
`message_id` / `topic` / `timestamp`. Recording is globally gated by
`RECORD_MESSAGES` regardless of backend.

## kraken_control — control-plane hooks (hot path only)

```erlang
-callback report_usage(Entries) -> ok | {ok, BlockedProjectIds} | {error, term()}.
-callback report_subscription_change(Changes) -> ok | {error, term()}.
-callback report_webhook_failure(Failure) -> ok.
```

Kraken core owns batching (usage: 30s/100-msg, subscriptions: 500ms/50-item)
and the quota-block cache; returning `{ok, Blocked}` from `report_usage`
replaces the blocked-project set (publishes from blocked projects get error
42920). The `http` built-in posts to `{CONTROL_HTTP_URL}/usage`,
`/subscriptions`, `/webhook-failures`.

**Not behaviours by design:** heartbeats, version polling, upgrade
orchestration. Embedding applications run those as their own supervised
sidecars and read `kraken:stats/0` (node, cluster nodes, connection counts,
configured backends, broker capabilities).
