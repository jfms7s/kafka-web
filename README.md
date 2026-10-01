# kafka-web

A local, single-user web UI for browsing and operating Apache Kafka clusters. FastAPI (Python 3.12)
and `confluent-kafka` on the back, React + TypeScript (Vite, Tailwind) on the front. Several clusters
can be open at once, each in its own browser tab.

- Topics: list, filter, config (human-readable sizes and durations), partition/replica/ISR summary.
- Messages: snapshot consume (earliest / latest / offset / timestamp), live stream over a WebSocket,
  single publish and bulk publish (CSV or JSON file). Text, JSON and binary (base64) payloads.
- Consumer groups: list, members, committed offsets and lag, create, reset offsets, delete.
- Browsing leaves no trace on the cluster: consumers use a throwaway `kafka-web-<uuid>` group id,
  `assign()` instead of `subscribe()`, and never commit.

Not goals: Schema Registry / Avro / Protobuf, topic create/delete, ACLs, multi-user or remote use.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 and the backend dependencies)
- Node 20 and npm
- A working OS keyring (Secret Service / GNOME Keyring / KWallet on Linux, Keychain on macOS). Needed
  as soon as you save a cluster with a SASL password.
- podman (or Docker) only for the dev Kafka stack and the integration tests

## Run

```bash
cd frontend && npm ci && cd ..   # once
./start.sh                       # dev: uvicorn --reload on :8000 + Vite on :5173 (open :5173)
./start.sh prod                  # builds the frontend, serves UI and /api from :8000
```

The server binds `127.0.0.1` only. Open <http://127.0.0.1:8000> (prod) or <http://localhost:5173>
(dev), add a cluster in the UI and connect.

### Security behaviour

- Only `127.0.0.1` and `localhost` are accepted as the `Host` header (TrustedHost), and every
  state-changing request (POST/PUT/DELETE) and the live-stream WebSocket must come from a same-origin
  page (`Origin` matches the `Host`). Together this defends against DNS rebinding and cross-site
  requests from other web pages in your browser. There is no login: anyone who can reach
  `127.0.0.1:8000` as you can use your clusters.
- Secrets (SASL password) live **only** in the OS keyring (service `kafka-web`, user
  `<cluster>:sasl_password`). There is no plaintext fallback: without a usable keyring, saving a
  cluster with a password fails with a clear error. No endpoint returns a secret and none is logged.
- Truststore passwords are used once for conversion and never stored.

## Configuration

Clusters are stored in `~/.config/kafka-web/clusters.yaml` (directory `0700`, files `0600`); it holds
no secrets and can be edited by hand. Set `KAFKA_WEB_CONFIG_DIR` to use another directory, e.g. for
a scratch run:

```bash
KAFKA_WEB_CONFIG_DIR=$(mktemp -d) ./start.sh prod
```

A cluster has a lowercase name (`^[a-z0-9][a-z0-9-]{0,62}$`, immutable), an `env` tag (`dev`, `stg`,
`prd`, ...), optional `region`, `bootstrap_servers`, `security_protocol`
(`PLAINTEXT` | `SSL` | `SASL_PLAINTEXT` | `SASL_SSL`), `sasl_mechanism` (`PLAIN` | `SCRAM-SHA-256` |
`SCRAM-SHA-512`), `sasl_username`, `read_only`, and optional raw librdkafka properties in `extra`
(keys owned by the typed fields, such as `bootstrap.servers` or `sasl.*`, are rejected).
Deleting a cluster removes its YAML entry, keyring secret and PEM file and closes its connection and
streams.

### Truststores

`SSL` and `SASL_SSL` clusters **require a truststore**; there is no fallback to system CAs. Upload it
in the form (or paste base64) in any of these formats: **JKS, JCEKS, PKCS12, DER (single
certificate) or PEM**. The format is detected from the content and converted once, at save time, to
a PEM bundle under `truststores/<cluster>.pem`, because librdkafka only reads PEM. Provide the
truststore password when the store needs one. A wrong password, an unknown format, or a file with
no certificates fails the save. A cluster whose PEM file is missing, or whose SASL secret is missing
from the keyring, is shown as *unusable* with the reason.

### Write protection

- A cluster with `read_only: true` (the default in the form for `env: prd`) rejects every write with
  `403 read_only_cluster`: publish (single and bulk), reset offsets, create group, delete group.
- Otherwise the destructive actions need a **typed confirmation** (the UI asks you to type the name;
  the API field is `confirm`): reset offsets and delete group take the group id, bulk publish takes the
  topic name. A mismatch is `422 confirmation_mismatch`.

### Limits and timeouts

- Bulk upload: 10 MB per file and 100,000 rows. A format starting with `[` is JSON, anything else is
  CSV.
- Snapshot consume: `count` 1 to 10,000, `timeout` 1 to 60 s. The request may take up to 10 s of
  setup (metadata, offsets) plus `timeout` of collection, and stops early once every partition has
  reached the end offset seen at the start.
- Live stream: a bounded queue (1,000 messages) drops the oldest messages under pressure and tells
  the client how many. If all brokers become unreachable the stream ends with an error frame instead
  of hanging.
- Every admin call has a 10 s timeout (`504 kafka_timeout`); other broker problems are
  `502 broker_unreachable` and similar.

## Dev Kafka stack

`dev/compose.yaml` runs one KRaft broker (`apache/kafka:3.9.1`) with two listeners on `127.0.0.1`:

| Port | Protocol | Credentials |
|---|---|---|
| 9092 | PLAINTEXT | none |
| 9094 | SASL_SSL, SASL/PLAIN | `app` / `app-secret` (also `admin` / `admin-secret`) |

Topic auto-creation is off; create topics with the broker's CLI. The ports are published on
`127.0.0.1` only, so librdkafka logs a harmless `Connect to ipv6#[::1]:9094 failed: Connection
refused` line before it falls back to IPv4.

```bash
dev/gen-certs.sh                                  # once: dev/certs/{ca.pem,broker.pem,truststore.jks}
uvx podman-compose -f dev/compose.yaml up -d      # no compose provider is installed by default
podman exec kafka-web-dev /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic demo --partitions 3
uvx podman-compose -f dev/compose.yaml down       # stop and remove
```

(`docker compose -f dev/compose.yaml ...` works the same.) `gen-certs.sh` refuses to overwrite
existing certificates unless you pass `--force`; it needs only `openssl` and the backend's `uv`
environment (the JKS is written with pyjks, no Java).

In the UI add two clusters:

- PLAINTEXT, bootstrap `localhost:9092`.
- SASL_SSL, bootstrap `localhost:9094`, mechanism `PLAIN`, user `app`, password `app-secret`,
  truststore `dev/certs/truststore.jks` with password `changeit` (this exercises JKS to PEM).

Notes: the SASL_SSL listener is named `EXTERNAL` in the broker config because the image entrypoint
refuses a PEM keystore on a listener whose name looks like `SSL://`/`SASL_*`; the security protocol
map still makes it SASL_SSL. The `:Z` flag on the certs mount relabels it for SELinux (Fedora).
Dev SASL uses `PLAIN` because the image cannot pre-provision SCRAM users simply; the app itself
supports all three mechanisms.

## Tests

```bash
scripts/check.sh                       # ruff + unit tests (backend), eslint + tsc + vitest (frontend)
cd backend && uv run pytest -m integration   # real brokers in containers
```

Integration tests start `apache/kafka:3.9.1` containers through Testcontainers and need a container
engine API. On Fedora with rootless podman, enable the socket once:

```bash
systemctl --user enable --now podman.socket
```

The tests default `DOCKER_HOST` to `unix:///run/user/1000/podman/podman.sock` (adjust if your UID is
not 1000) and disable the Ryuk reaper. Pull the image beforehand (`podman pull apache/kafka:3.9.1`).
Unit tests need neither Kafka nor a real keyring.

## Layout

```
backend/    FastAPI app (kafka_web/), tests/{unit,integration}
frontend/   Vite + React app; API types generated with `npm run gen:api`
dev/        compose.yaml, gen-certs.sh (local Kafka, certs)
scripts/    check.sh
start.sh
```
