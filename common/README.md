# common

Shared code for the platform services: a typed message bus (Kafka message kinds), typed object storage (key layout + validated encoding), log line types, and `.env` loading.

Services use typed handles. The concrete adapter is chosen at startup:

| Handle                      | Real adapter                                 | Mock adapter                            |
| --------------------------- | -------------------------------------------- | --------------------------------------- |
| `bus::Publisher<M>`         | `Publisher::kafka(&KafkaProducer)`           | `Publisher::memory(&MemoryBus)`         |
| `bus::Subscriber<M>`        | `Subscriber::<M>::kafka(brokers, group_id)`  | `Subscriber::<M>::memory(&MemoryBus)`   |
| `storage::ObjectStorage<O>` | `ObjectStorage::s3(&S3Config)` (Garage / S3) | `ObjectStorage::memory(&MemoryStorage)` |

Bus and storage follow the same layout:

```
bus/                         storage/
  mod.rs   Publisher<M>,       mod.rs   ObjectStorage<O>
           Subscriber<M>,
           Delivery<M>
  backend/ kafka, memory       backend/ s3, memory     (raw traits are private to the module)
  messages/                    objects/
    cli_run.rs                   artifact.rs
    http_deploy.rs               logs.rs
    run_event.rs
```

## Typed bus

Each message kind has its own file in `bus/messages/`. That file holds the struct and its `impl Publisher<Kind>` / `impl Subscriber<Kind>` blocks, which fix the topic, the partition key (always `app_id`) and the JSON encoding:

| Kind                      | Topic         | Published by                       | Consumed by   |
| ------------------------- | ------------- | ---------------------------------- | ------------- |
| `cli_run::CliRun`         | `cli.run`     | control plane (manual + scheduler) | runner-cli    |
| `http_deploy::HttpDeploy` | `http.deploy` | control plane                      | runner-http (every replica; own group, replayed from the start) |
| `run_event::RunEvent`     | `run.events`  | runners                            | control plane |

- **`Publisher<M>::publish(&M)`**: create one `KafkaProducer` per process and build every publisher from it, so they share one connection.
- **`Subscriber<M>`**: `recv()` returns a `Delivery<M>` whose `message` is `Result<M, BusError>`, so a poison message can still be committed and skipped. `commit(&delivery)` marks it processed (at-least-once). Each subscriber joins the consumer group on its kind's topic. Its constructors are per kind, so name the kind: `Subscriber::<CliRun>::kafka(..)`.
- The raw producer/consumer traits are private to `bus`. Other crates cannot send arbitrary payloads to arbitrary topics. To add a new kind, create a file in `bus/messages/` with its `impl` blocks.
- `MemoryBus` test helpers: `push(topic, &msg)`, `push_raw(..)` (for poison messages), `published::<M>(topic)`, `committed(topic)`, `pending(topic)`.

## Typed storage

`ObjectStorage<O>` is a handle to one bucket holding objects of kind `O`. Each kind is a marker struct, and its operations are implemented in an `impl ObjectStorage<Kind>` block that takes that kind's own key and contents types. Validation and byte encoding happen inside those methods:

| Kind                          | Key → path                                            | Contents    | Validation / encoding                     |
| ----------------------------- | ----------------------------------------------------- | ----------- | ----------------------------------------- |
| `objects::artifact::Artifact` | `ArtifactKey` → `apps/<app>/<version>/component.wasm` | `Bytes`     | must start with the wasm component header |
| `objects::logs::CliLogChunk`  | `CliLogKey` → `cli/<app>/<run>/<seq>.log.gz`          | `[LogLine]` | non-empty; gzip NDJSON                    |
| `objects::logs::HttpLogChunk` | `HttpLogKey` → `http/<app>/<yyyy-mm-dd>/<hh>/<writer>-<seq>.log.gz` | `[LogLine]` | non-empty, valid writer (`[A-Za-z0-9._-]`); gzip NDJSON |

Only the constructors (`s3`, `memory`) are generic. The raw string/bytes backend trait is crate-private, so no other crate can upload unvalidated bytes. To add a new kind, create the marker struct and key type, then write an `impl ObjectStorage<NewKind>` block in `common`. `ArtifactKey` (de)serializes as its path, so Kafka messages with a malformed key fail to decode.

## S3 clients and credentials

**Decision:** every service opens one S3 client **per bucket** (artifacts, logs). Each client has its own HTTP connection pool and **optionally its own credentials**, and does not share connections with the other buckets' clients.

- **Why separate clients:** the bucket is part of the client configuration (`object_store`), so one client per bucket is natural. It keeps log uploads from using up the connections that artifact fetches need. Sharing would only save a few idle connections to the same Garage endpoint, which is negligible at our request rates.
- **Why per-bucket credentials:** least privilege. Runners get a read-only key for artifacts and a write-only key for logs. The control plane gets read/write on artifacts (uploads) and read/delete on logs (serving, retention). If a runner is compromised, it can neither overwrite artifacts nor read other apps' logs.
- **Configuration:** `<BUCKET>_S3_ACCESS_KEY` / `<BUCKET>_S3_SECRET_KEY` (e.g. `LOG_S3_ACCESS_KEY`) override the shared `S3_ACCESS_KEY` / `S3_SECRET_KEY`. Set both of a pair or neither. Endpoint and region stay shared. Local dev uses only the shared pair. Implemented by `common::env::s3_bucket_config`.

## Features

- `kafka` (default): rdkafka-based adapter; builds librdkafka from source.
- `s3` (default): `object_store`-based S3 adapter.
- `mock`: the in-memory adapters, each only alongside its real adapter (`MemoryBus` needs `kafka`, `MemoryStorage` needs `s3`). Enable it **only** in `[dev-dependencies]`:

  ```toml
  [dependencies]
  common = { path = "../common" }

  [dev-dependencies]
  common = { path = "../common", features = ["mock"] }
  ```

  With resolver 3, a feature turned on by dev-dependencies is active only when building tests, examples and benches, so release binaries never contain the mocks.

## Env loading

`env::load_dotenv(env!("CARGO_MANIFEST_DIR"))` loads, highest precedence first: the process env, `<crate>/.env.local`, `<crate>/.env`, `<root>/.env.local`, `<root>/.env`.
