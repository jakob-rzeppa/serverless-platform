# runner-http

Serves HTTP apps: WASM components that export `wasi:http/incoming-handler`. It consumes `http.deploy` to learn which app version is served at which subdomain, routes every request by its `Host` header (`<subdomain>.<APPS_DOMAIN>`), and runs it in a fresh Wasmtime instance. Guest stdout/stderr goes to the logs bucket, tagged with the request id. runner-http publishes no Kafka messages.

## How it works

### Deploys

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka (http.deploy)
    participant D as Deployer
    participant C as ArtifactCache<br/>(CACHE_DIR)
    participant A as S3 artifacts bucket
    participant T as AppTable

    Note over K,D: own consumer group per process start, replayed from the earliest offset
    loop until SIGINT / SIGTERM
        K->>D: recv http.deploy
        alt message cannot be decoded (poison)
            D->>D: log, skip
        else Deploy{app, version, subdomain, artifact, sha256, env}
            D->>C: component(artifact_key, sha256)
            alt #lt;sha256#gt;.wasm on disk and hash matches
                C->>C: read from disk
            else cache miss or corrupt file
                C->>A: get artifact_key
                A-->>C: bytes
                C->>C: verify sha256, write file atomically
            end
            C-->>D: compiled Component
            D->>D: ProxyPre (pre-instantiate, must export wasi:http/incoming-handler)
            alt any step failed (or invalid subdomain)
                D->>D: log error, previous version keeps serving
            else ready
                D->>T: insert subdomain → app (in-flight requests finish on the old version)
            end
        else Remove{app}
            D->>T: remove app (no-op if unknown)
        end
        D->>K: commit offset
    end
```

- **Every replica serves every app**, so every replica must see every `http.deploy` message. Each process start uses its own consumer group (`runner-http.<pod>.<random>`) and reads the topic from the earliest offset to rebuild the routing table. The topic should be log-compacted by `app_id` so this replay stays short. Because of this, offsets are committed only as a progress marker: a failed commit is logged, not fatal.
- `Deploy` fetches the artifact (verified by sha256, cached on disk), compiles it, and pre-instantiates it (`ProxyPre`). Only then does the new version replace the old one in the table. Requests already running on the old version finish on it.
- A changed `subdomain` frees the old one. If another app already holds the subdomain, the latest deploy wins and a warning is logged.
- **A deploy that fails** (artifact not found, hash mismatch, not a component, no `wasi:http/incoming-handler` export, invalid subdomain) is logged, and the app keeps serving its previous version, if it had one.
- `Remove` stops serving the app. Removing an unknown app does nothing.
- Undecodable (poison) messages are logged and skipped.

### Requests

```mermaid
sequenceDiagram
    autonumber
    actor U as Client
    participant S as Server
    participant T as AppTable
    participant G as Guest instance
    participant L as LogBook
    participant B as S3 logs bucket

    U->>S: GET / (Host: hello.apps.example.com)
    S->>T: get("hello")
    alt no app at this host
        S-->>U: 404
    else app found
        S->>G: fresh store + instance, x-request-id header, call handle
        alt guest sets a response
            G-->>S: response head (body may still stream)
            S-->>U: guest's response + x-request-id
        else trap or error code
            S-->>U: 500
        else timeout
            S-->>U: 504
        end
        G->>L: stdout/stderr lines tagged with request_id (when the guest finishes)
    end
    loop every LOG_FLUSH_SECS, or when an app buffers LOG_FLUSH_BYTES
        L->>B: put http/#lt;app#gt;/#lt;date#gt;/#lt;hh#gt;/#lt;writer#gt;-#lt;seq#gt;.log.gz
    end
```

| Request                                                                              | Response                                               |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------ |
| `Host: <subdomain>.<APPS_DOMAIN>` (case-insensitive, any port), app deployed         | the guest's response                                   |
| subdomain not deployed, nested subdomain (`a.b.<domain>`), or `<APPS_DOMAIN>` itself | `404`                                                  |
| any other host, path `/healthz`                                                      | `200 ok` (for liveness/readiness probes on the pod IP) |
| any other host, any other path                                                       | `404`                                                  |
| guest traps (panic, out of memory) or returns an error code                          | `500 internal error`                                   |
| guest exceeds `REQUEST_TIMEOUT_SECS`                                                 | `504 request timed out`                                |

- Every response carries `x-request-id`. The guest sees the same value in its request's `x-request-id` header, and a client-supplied value is replaced. Error responses never include guest details; those go to the app's logs as `system` lines with the request id.
- `x-forwarded-proto: https` (set by the gateway) makes the guest see the `https` scheme. Otherwise the guest sees `http`.
- **Sandbox per request:** a fresh store and instance, created from the pre-instantiated component. It gets the deploy's `env`, a `MEMORY_LIMIT_MB` memory cap, no preopened directories and no sockets. **Outgoing HTTP is denied**: `wasi:http/outgoing-handler` is linked, so components that import it still load, but every request fails without opening a connection.
- **Timeout:** the epoch deadline covers the whole request, including streaming the response body. A CPU-bound guest yields to the executor every 100 ms, so it can't starve other requests on the same worker thread. A guest blocked in a host call is stopped by a backstop 5 s after the timeout.
- Only HTTP/1.1 is served. TLS, HTTP/2, rate limits and body size limits are handled by the gateway.

### Logs

Log key: `http/<app_id>/<yyyy-mm-dd>/<hh>/<writer>-<seq>.log.gz` (gzip NDJSON `LogLine`s, see `common::storage::objects::logs::HttpLogChunk`). `writer` is `<pod>.<random>`, unique per process start, so replicas and restarts never overwrite each other's chunks. `seq` counts that writer's chunks for the app.

- A request's output is captured into its own buffer (up to `LOG_REQUEST_MAX_BYTES` per stream, then a `[stdout truncated: output limit reached]` marker). It is handed to the app's log buffer when the request ends, so output from a still-running request is not visible yet.
- Each app may log `LOG_APP_MAX_BYTES_PER_HOUR` bytes per UTC hour. Beyond that, lines are dropped and one `[log limit for this hour reached: further output dropped]` marker is written for the rest of the hour. Platform (`system`) lines don't count against the limit.
- Buffers are uploaded every `LOG_FLUSH_SECS`, or earlier when an app's buffer reaches `LOG_FLUSH_BYTES`. Each upload writes one chunk per app and hour. Upload failures are logged, and those lines are lost.

### Shutdown

On SIGINT/SIGTERM the server stops accepting connections and waits up to `REQUEST_TIMEOUT_SECS + 10 s` for in-flight requests (and their guest tasks). It then stops consuming deploys and uploads the remaining logs.

## Run

```sh
cargo run -p runner-http   # needs Kafka + Garage, see docs/design.md → Local Development
curl http://hello.apps.localhost:8080/   # after deploying an app with subdomain "hello"
cargo test -p runner-http  # no external services: in-memory Kafka + S3 from common, guest in test-guest/
```

## Configuration

Shared variables come from `<root>/.env` (`KAFKA_BROKERS`, `S3_*`, `ARTIFACT_BUCKET`, `LOG_BUCKET`, `RUST_LOG`). Runner-specific variables come from `runner-http/.env`. `.env.local` overrides either file. Each bucket gets its own S3 client. Credentials default to the shared `S3_ACCESS_KEY` / `S3_SECRET_KEY` and can be overridden per bucket with `ARTIFACT_S3_ACCESS_KEY` / `ARTIFACT_S3_SECRET_KEY` and `LOG_S3_ACCESS_KEY` / `LOG_S3_SECRET_KEY` (set both of a pair or neither). In production, give runner-http a read-only artifacts key and a write-only logs key.

| Var                          | Default                   |                                                                                   |
| ---------------------------- | ------------------------- | --------------------------------------------------------------------------------- |
| `APPS_DOMAIN`                | required                  | apps are served at `<subdomain>.<APPS_DOMAIN>` (dev: `apps.localhost`)            |
| `LISTEN_ADDR`                | `0.0.0.0:8080`            |                                                                                   |
| `POD_NAME`                   | `$HOSTNAME`, else `local` | replica name, used in the consumer group and log writer id; `[A-Za-z0-9._-]` only |
| `CACHE_DIR`                  | `/var/cache/runner-http`  | sha256-keyed artifact cache                                                       |
| `REQUEST_TIMEOUT_SECS`       | `30`                      | wall-clock limit per request                                                      |
| `MEMORY_LIMIT_MB`            | `128`                     | linear memory per request instance                                                |
| `LOG_REQUEST_MAX_BYTES`      | `1048576`                 | per stream and request; excess dropped with a marker                              |
| `LOG_APP_MAX_BYTES_PER_HOUR` | `104857600`               | per app and UTC hour; excess dropped with a marker                                |
| `LOG_FLUSH_SECS`             | `5`                       | upload interval                                                                   |
| `LOG_FLUSH_BYTES`            | `1048576`                 | upload an app's buffer early at this size                                         |

## Code layout

| Module   |                                                                                                            |
| -------- | ---------------------------------------------------------------------------------------------------------- |
| `runner` | `Runner`: wires the deployer, the HTTP server and the log uploader; graceful shutdown                      |
| `deploy` | `Deployer`: `http.deploy` → `AppTable`                                                                     |
| `apps`   | `AppTable`: subdomain → `Arc<App>` (version, env, `ProxyPre`)                                              |
| `server` | `Host` routing, request id, per-request instance, error → status mapping                                   |
| `guest`  | store data (`GuestState` wraps runner-core's `HostState` + `WasiHttpCtx`), linker, outgoing-request denial |
| `logs`   | `LogBook`: per-app buffering, hourly budget, chunk upload                                                  |

Engine, store limits, timeouts, the artifact cache and output capture come from [`runner-core`](../runner-core/README.md).

## Tests

`tests/serve.rs` starts the real `Runner` on a TCP port against `MemoryBus` and `MemoryStorage` from `common` (feature `mock`). Each test acts like the control plane and a client:

- it uploads [`test-guest`](test-guest/src/main.rs) through the typed artifact store and publishes `http.deploy` messages, waiting until each is committed
- it sends HTTP requests with a `Host` header
- it checks the responses, the uploaded log chunks and the committed offsets

The request path picks the guest's behavior (`/env/<name>`, `/echo`, `/status/<code>`, `/log`, `/spin`, `/panic`, `/alloc/<mib>`, `/fetch`, `/sleep/<ms>`, …). The tests build the guest themselves, which needs the `wasm32-wasip2` target.
