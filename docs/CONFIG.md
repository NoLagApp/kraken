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
| `CLUSTER_DNS_NAME` | — | DNS name resolving to peer IPs (dns) |
| `CLUSTER_HOSTS` | — | comma-separated node names (epmd) |
| `CLUSTER_GOSSIP_PORT` / `CLUSTER_GOSSIP_SECRET` | `45892` / — | gossip multicast |
| `ERLANG_NODE_NAME` | `kraken@127.0.0.1` | **longnames: host part must be an FQDN or IP** |
| `ERLANG_COOKIE` | `kraken_dev_cookie` | must match across the cluster |
