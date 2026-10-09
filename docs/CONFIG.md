# Configuration Reference

All settings are environment variables (substituted into `sys.config` at
release start).

## Listeners

| Var | Default | Meaning |
|-----|---------|---------|
| `WS_PORT` | `8080` | WebSocket + HTTP (health, internal publish) |
| `MQTT_PORT` | `1883` | MQTT ingress listener |

## Backends

| Var | Default | Values |
|-----|---------|--------|
| `AUTH_BACKEND` | `static` | `static`, `http`, custom module |
| `BROKER_BACKEND` | `syn` | `syn`, `mqtt`, custom module |
| `STORE_BACKEND` | `ets` | `ets`, `noop`, custom module |
| `CONTROL_BACKEND` | `noop` | `noop`, `http`, custom module |

## Auth

| Var | Default | Meaning |
|-----|---------|---------|
| `AUTH_FILE` | `/app/examples/auth.json` | token file for `static` |
| `AUTH_ALLOW_ALL` | `false` | dev mode: accept any token, full access (**INSECURE**) |
| `AUTH_HTTP_URL` | — | base URL for the `http` auth backend |
| `BACKEND_SECRET` | — | bearer secret sent to auth/control HTTP backends |

## MQTT broker backend

| Var | Default | Meaning |
|-----|---------|---------|
| `MQTT_BROKER_HOST` / `MQTT_BROKER_PORT` | — / `1884` | external broker address |
| `MQTT_BROKER_USERNAME` / `MQTT_BROKER_PASSWORD` | — | optional credentials |

## Store / recording

| Var | Default | Meaning |
|-----|---------|---------|
| `RECORD_MESSAGES` | `true` | master switch for history/replay recording |
| `STORE_TTL_SECONDS` | `3600` | ets store: message retention |
| `STORE_MAX_MESSAGES` | `10000` | ets store: bound before pruning oldest |

## Limits

| Var | Default | Meaning |
|-----|---------|---------|
| `MAX_MESSAGE_SIZE` | `921600` | default payload ceiling in bytes (auth backends may override per token) |

## Control plane

| Var | Default | Meaning |
|-----|---------|---------|
| `CONTROL_HTTP_URL` | — | base URL for the `http` control backend |

## HTTP internal publish

| Var | Default | Meaning |
|-----|---------|---------|
| `INTERNAL_SECRET` | `change_me` | shared secret for `POST /internal/publish` |

## Clustering

| Var | Default | Meaning |
|-----|---------|---------|
| `CLUSTER_STRATEGY` | `standalone` | `standalone`, `dns`, `epmd`, `gossip` |
| `CLUSTER_DNS_NAME` | (unset) | DNS name whose A records are the peer IPs (dns). `CLUSTER_DNS_QUERY` is accepted as an alias |
| `CLUSTER_NODE_BASENAME` | name part of this node's `ERLANG_NODE_NAME` | dns: peers are dialled as `<basename>@<ip>` |
| `CLUSTER_HOSTS` | (unset) | comma-separated node names (epmd) |
| `CLUSTER_GOSSIP_PORT` | `45892` | UDP port for gossip announces |
| `CLUSTER_MULTICAST_ADDR` | `230.1.1.1` | multicast group for gossip announces |
| `CLUSTER_GOSSIP_SECRET` | (unset) | key that signs gossip announces (HMAC-SHA256); the cookie is used when unset |
| `CLUSTER_POLL_INTERVAL` | `30000` | ms between dns/epmd polls and gossip re-announces |
| `ERLANG_NODE_NAME` | `kraken@127.0.0.1` | **longnames: host part must be an FQDN or IP** |
| `ERLANG_COOKIE` | `kraken_dev_cookie` | must match across the cluster; never logged |

- `dns`: name every node `<basename>@<its own IP>` (in Kubernetes,
  `ERLANG_NODE_NAME=kraken@$(POD_IP)` with a headless Service as
  `CLUSTER_DNS_NAME`), because peers are dialled at the IPs the name returns.
- `gossip`: UDP multicast with TTL 1, so nodes must share a network segment
  that carries multicast. A user-defined Docker bridge does; most cloud VPCs
  do not. An announce only tells peers where to dial: joining still needs the
  same `ERLANG_COOKIE`.
- The cookie grants full remote code execution on every node. Set a long
  random value and keep epmd (4369) and the distribution ports (9100-9200)
  off untrusted networks.
