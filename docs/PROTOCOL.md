# Kraken Wire Protocol

WebSocket at `/ws`, **binary frames containing MessagePack** (packed with
`str` from binary / unpacked `str` as binary). The protocol is identical to
the NoLag cloud protocol — official NoLag SDKs work against kraken unchanged.

Heartbeat: an **empty binary frame** in either direction; the server echoes
when authenticated.

## Authentication (first message)

```jsonc
// client -> server
{ "type": "auth", "token": "<access token>", "reconnect": false }

// success
{ "type": "auth", "success": true, "actorTokenId": "...", "projectId": "...",
  "actorType": "...", "restoredSubscriptions": ["pattern", ...] }

// failure
{ "type": "auth", "success": false, "error": "access_denied" | "connection_limit_reached" | "broker_unavailable" }
```

## Client → server messages

| type | fields |
|------|--------|
| `publish` | `topic`, `data`, `echo` (default true), `qos` (0-2, default 1), `retain` (default false), `filter` / `filters` |
| `subscribe` | `topic`, `qos`, `loadBalance`, `loadBalanceGroup`, `filters` |
| `unsubscribe` | `topic` |
| `setFilters` | `topic`, `filters` |
| `presence` | `roomId` (slug), `data` |
| `getPresence` | `roomId` |
| `lobbySubscribe` / `lobbyUnsubscribe` / `getLobbyPresence` | `lobbyId` (slug) |
| `ack` / `batchAck` | `msgId` / `msgIds` |

Responses: `subscribed`, `unsubscribed`, `filtersUpdated`,
`presenceList` (`roomId`, `data`), `lobbyPresenceList`.

## Server → client messages

```jsonc
// topic message
{ "type": "message", "topic": "...", "data": ..., "msgId": "...",
  "requiresAck": true, "filter": "...", "isReplay": true }

// presence events
{ "type": "presence", "event": "join"|"leave"|"update",
  "data": { "actor_token_id": "...", "presence": {...} } }
{ "type": "lobbyPresence", "event": "...", "lobbyId": "...", "roomId": "...",
  "actorId": "...", "data": {...} }

// hydration (webhook-sourced data on subscribe)
{ "type": "hydration", "topic": "...", "data": {...} }

// replay framing
{ "type": "replayStart", "count": N, "oldestTimestamp": T, "newestTimestamp": T }
{ "type": "replayEnd", "count": N }

// errors
{ "type": "error", "code": C, "error": "...", "topic": "..." }
```

## Error codes

| Code | Error |
|------|-------|
| 42910 | `rate_limit_exceeded` (per-connection msg/s limit) |
| 42920 | `monthly_quota_exceeded` (control-plane block) |
| 42930 | `message_too_large` (includes `maxSizeBytes`; payload measured as packed msgpack) |

## Echo suppression

`echo: false` publishes wrap the payload as
`{ "data": ..., "_sender": "<connectionId>" }` on the broker; the sender's
own connection drops it on delivery. With delivery tracking active, payloads
carry `{ "_msgId": "...", "_data": ... }` envelopes that the server unwraps
into `msgId`/`requiresAck` before forwarding.

## MQTT ingress

Devices can connect over MQTT 3.1.1 (default port 1883): CONNECT
username/password = anything/`<access token>` flows through the same auth
backend, topics map to ACL patterns, QoS 0-2 supported.
