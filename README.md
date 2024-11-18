Skynet
=============================

**Skynet** is a service designed for advanced model management, transformation, ranking, and visualization. It supports dynamic APIs for working with machine learning models and provides extensive health checks, metrics, and documentation.

---

## Features

- **Model Loading & Management**: Load, unload, and list models dynamically via API.
- **Data Transformation**: Transform data using preloaded models.
- **Ranking Framework**: Rank model outputs with customizable parameters.
- **Visualization**: Generate computation graphs of loaded models.
- **Health Checks**: Monitor both service and model health.
- **Swagger Documentation**: Auto-generated OpenAPI specs with Swagger UI.
- **Metrics**: Comprehensive monitoring and metrics for endpoints.

---

## Getting Started

### Prerequisites

- **Scala 2.13.** and **sbt** for building and running the service.
- **Docker** for containerization (optional).
- Dependencies are managed in the SBT build file.

---

## Application Structure

- **Main Entry Point**: [`Starter.scala`](skynet-api/src/main/scala/com/overstock/skynet/Starter.scala)
  - Bootstraps the service, initializes configurations, and starts the server.
  - Uses ZIO for dependency injection and environment management.

- **API Definition**: [`Endpoints.scala`](skynet-api/src/main/scala/com/overstock/skynet/http/Endpoints.scala)
  - Defines RESTful API endpoints using Tapir.
  - Includes model management, transformation, and health check APIs.

- **Routing**: [`Routes.scala`](skynet-api/src/main/scala/com/overstock/skynet/http/Routes.scala)
  - Maps endpoints to route handlers and integrates Swagger for API documentation.
  - Includes middleware for metrics and error handling.

- **Build Configuration**: [`build.sbt`](build.sbt)
  - Handles project dependencies, build plugins, and Docker configurations.

---

## Key API Endpoints

### Model Operations

- **Load a Model**  
  `PUT /models/{model}`  
  Loads a model from a given URI (e.g., `file://`, `s3://`).

- **Unload a Model**  
  `DELETE /models/{model}`  
  Unloads a model from the service.

- **List All Models**  
  `GET /models`  
  Lists all currently loaded models.

- **Model Health Check**  
  `GET /models/{model}/health`  
  Performs a test prediction to verify the model is operational.

### Data Operations

- **Transform Data**  
  `POST /models/{model}/transform`  
  Transforms input data using the specified model.

- **Rank Data**  
  `POST /models/{model}/rank`  
  Ranks transformed data with options for grouping and averaging.

- **Get Sample Data**  
  `GET /models/{model}/sample`  
  Returns a sample data frame for a model.

### Visualization

- **Model Graph**  
  `GET /models/{model}/graph`  
  Visualizes the computation graph of the model.

### Service Health

- **Service Health Check**  
  `GET /health`  
  Confirms the service is operational.

---

## Build & Run Instructions

### Build

```bash
sbt compile
```

### Run

```bash
sbt run
```

The service will start and provide API access via `http://localhost:8080`.

### Docker Build

A Docker image can be built using:

```bash
sbt docker:publishLocal
```

This will create a containerized version of the service.

---

## Dependencies

### Core Libraries

- **ZIO**: Functional effect handling.
- **Tapir**: API definition and OpenAPI documentation.
- **Http4s**: Web server and client.

### Machine Learning

- **MLeap**: Runtime and executor for machine learning pipelines.
- **XGBoost**: Predictor integration. The implementation here is much faster than MLeaps original implementation. This allows for BLAZINGLY FAST XGBoost serving!

### Configuration

- **PureConfig**: Simplified configuration management.
- **Typesafe Config**: Configuration library.

### JSON & Serialization

- **Circe**: JSON serialization and parsing.
- **Protobuf**: Support for protocol buffers.

### Logging

- **Logback**: Logging framework.
- **Scala Logging**: Integration for logging in Scala.

### Documentation

- **Swagger UI**: Interactive API documentation.
- **RefTree**: Visualization of computation graphs.

### Metrics

- **Prometheus**: Metrics collection and integration.

---

## API Documentation

Swagger UI is available at:
`http://localhost:8080/swagger-ui`

This provides an interactive interface to explore and test the API.

---

## Metrics

Metrics are exposed at:
`http://localhost:8080/metrics`

These include endpoint-specific metrics and overall service health data.

---

## Contributing

Feel free to contribute by submitting issues or pull requests. Follow functional programming principles and the existing code style.
