# Skynet

Skynet is an open-source Scala runtime for serving portable [MLeap](https://mleap-docs.combust.ml/) model bundles. It owns the part of the lifecycle after training: acquiring an artifact, loading and warming its transformer graph, accepting structured frames, transforming or ranking results, and exposing the evidence needed to operate the service.

**[Read the illustrated documentation](https://adrielc.github.io/skynet/)**

> **Project status:** This repository is a public reference implementation built on Scala 2.12.9, ZIO 1, HTTP4s, Tapir, and MLeap 0.16. It is not a managed hosted service. Review and update dependencies, security controls, and deployment policy before using it for new production workloads.

## What it does

- Registers and unloads named model bundles at runtime.
- Acquires bundles from local files, Amazon S3, or Google Cloud Storage.
- Accepts Leap, Cartesian, Context, and Prefixed frame shapes.
- Transforms frames with sequential, parallel, or bounded-parallel execution.
- Ranks output by an expression, with top-k, grouping, averaging, and field selection.
- Generates schema-correct sample data and warms models with real transformations.
- Exposes model metadata, health checks, computation graphs, Swagger, and Prometheus metrics.

## Architecture

```text
bundle URI
   │
   ▼
repository adapter ── file / S3 / GCS
   │
   ▼
model registry ── cached MLeap transformer
   │
   ├── sample / graph / health
   │
   ▼
frame decoder ── transform ── select / rank ── HTTP response
                                  │
                                  └── Prometheus metrics
```

The HTTP surface is defined once with Tapir. The same definitions drive request decoding and the OpenAPI document served through Swagger UI. ZIO manages effects and runtime layers; HTTP4s serves the routes; MLeap executes the portable transformer graph.

## Run locally

Prerequisites: JDK 11, sbt 1.x, and an MLeap bundle you can access.

```bash
git clone https://github.com/AdrielC/skynet.git
cd skynet
HTTP_PORT=8080 sbt run
```

The checked-in default is port `80`; `HTTP_PORT=8080` avoids requiring a privileged port for local development.

Verify the process:

```bash
curl http://localhost:8080/health
```

Register a model bundle. The request body is its URI:

```bash
curl -X PUT \
  http://localhost:8080/models/recommender \
  --header 'Content-Type: text/plain' \
  --data 's3://models/production/recommender.zip'
```

Then open `http://localhost:8080/docs` for Swagger or fetch a schema-correct input example:

```bash
curl http://localhost:8080/models/recommender/sample
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `PUT` | `/models/{model}` | Register a bundle URI under a model name. |
| `DELETE` | `/models/{model}` | Evict the model and invalidate its cached transformer. |
| `GET` | `/models/{model}` | Read bundle origin, metadata, and schemas. |
| `GET` | `/models` | List registered model names. |
| `POST` | `/models/{model}/transform` | Transform a frame; optionally select fields and rank top-k rows. |
| `POST` | `/models/{model}/rank` | Transform and rank identifiers, with grouping and score averaging. |
| `GET` | `/models/{model}/sample` | Generate input matching the model schema. |
| `GET` | `/models/{model}/graph` | Render the transformer graph as SVG. |
| `GET` | `/models/{model}/health` | Run generated samples through one model. |
| `GET` | `/health` | Check that the service and model registry respond. |

### Execution and missing-data controls

Transform and rank requests accept an `exec` query parameter:

- `seq` — transform rows sequentially.
- `par` — use unbounded ZIO parallelism.
- `par-N` — cap parallelism at `N`, for example `par-8`.

The `missing` parameter is either `impute` (the default) or `error`. Imputation uses an empty value appropriate to the missing field type.

## Operations

- `/docs` — bundled Swagger UI.
- `/metrics` — Prometheus metrics for request totals, failures, latency, and executor pools.
- `/health` — service-level health.
- `/models/{model}/health` — transformation-level health for one model.
- `/models/{model}/graph` — computation graph rendering; the Docker image installs Graphviz.

The model cache holds five transformers for 30 minutes by default. The runtime also supports environment overrides for HTTP behavior, model execution strategy, warmup row count, and executor sizing; see [`reference.conf`](skynet-api/src/main/resources/reference.conf) for the complete configuration surface.

## Build and test

```bash
sbt test
sbt assembly
sbt docker:publishLocal
```

The Docker build packages the assembly on JDK 11 and installs the native libraries required by XGBoost and graph rendering.

## Source map

- [`Starter.scala`](skynet-api/src/main/scala/com/overstock/skynet/Starter.scala) — process entry point and runtime layers.
- [`Endpoints.scala`](skynet-api/src/main/scala/com/overstock/skynet/http/Endpoints.scala) — Tapir endpoint definitions.
- [`Routes.scala`](skynet-api/src/main/scala/com/overstock/skynet/http/Routes.scala) — route interpreters and middleware.
- [`Frame.scala`](skynet-api/src/main/scala/com/overstock/skynet/domain/Frame.scala) — supported frame shapes and codecs.
- [`ModelService.scala`](skynet-api/src/main/scala/com/overstock/skynet/service/model/ModelService.scala) — transform and ranking orchestration.
- [`reference.conf`](skynet-api/src/main/resources/reference.conf) — runtime configuration and defaults.

## License

[Apache License 2.0](LICENSE)
