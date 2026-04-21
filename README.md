# Canton Event Streaming Connector

[![Build Status](https://img.shields.io/github/actions/workflow/status/digital-asset/canton-event-streaming/ci.yml?branch=main)](https://github.com/digital-asset/canton-event-streaming/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Release](https://img.shields.io/github/v/release/digital-asset/canton-event-streaming)](https://github.com/digital-asset/canton-event-streaming/releases)
[![Scala Version](https://img.shields.io/badge/scala-2.13-red.svg)](https://www.scala-lang.org/)

A production-grade, high-performance service to stream events from a Canton ledger participant node into enterprise data pipelines like Kafka, Pulsar, and AWS Kinesis.

This connector enables data teams to integrate privacy-preserving Canton applications with existing analytics platforms, data lakes, and event-driven microservices without writing custom plumbing. It's designed for reliability, scalability, and operational observability.

---

## Overview

Canton is a privacy-enabled distributed ledger, meaning each participant only sees a subset of the network's transactions. There is no central, queryable database of all ledger activity. To integrate Canton-based applications with downstream systems, each organization must stream its own view of the ledger from its participant node.

The Canton Event Streaming Connector solves this problem by:

1.  **Connecting** to a Canton participant's Ledger API stream.
2.  **Transforming** ledger events (contract creations, choices, etc.) into structured data formats (e.g., JSON, Avro).
3.  **Publishing** these events to a message broker topic or stream in real-time.
4.  **Persisting** its progress, guaranteeing exactly-once delivery and resilience to failures.

This allows you to seamlessly feed Canton ledger data into systems like Snowflake, Databricks, Elastic, or any custom microservice that consumes from your event backbone.

## Features

*   **Reliable Delivery**: At-least-once and exactly-once delivery semantics to prevent data loss or duplication.
*   **Resilient**: Resumes streaming from the last known position after restarts or disconnects.
*   **High Performance**: Built on a non-blocking, asynchronous stack (Akka Streams) to handle high-throughput ledgers.
*   **Backpressure Aware**: Automatically adjusts consumption speed to avoid overwhelming downstream systems.
*   **Pluggable Sinks**:
    *   [x] **Apache Kafka**
    *   [x] **Apache Pulsar**
    *   [x] **AWS Kinesis**
*   **Flexible Filtering**: Configure the connector to stream events only for specific parties, templates, or interfaces.
*   **Schema Integration**: Integrates with Confluent Schema Registry for robust Avro/Protobuf schema management.
*   **Production Ready**:
    *   Exposes detailed Prometheus metrics for monitoring.
    *   Structured JSON logging for easy integration with log aggregators.
    *   Configurable Dead-Letter Queue (DLQ) for failed messages.
    *   Health check endpoints for container orchestrators.
*   **Secure**: Supports TLS for secure connections to both the Canton participant and the event sink.

## Architecture

The connector operates as a standalone service that you deploy within your infrastructure. It maintains a persistent connection to your Canton participant node.

```
                                      ┌───────────────────────────────┐
                                      │  Canton Event Streaming       │
                                      │         Connector             │
                                      │                               │
+------------------------+   gRPC     │  ┌──────────┐   ┌───────────┐ │  ┌───────────┐   +-----------------+
| Canton Participant     | ---------> │  │  Source  │   │ Transform │ │  │   Sink    │   | Apache Kafka /  |
| Node                   | Ledger API │  │ (Canton) │ > │ (Filter)  │ │> │ (Kafka)   │=> | Pulsar / Kinesis|
+------------------------+            │  └──────────┘   └───────────┘ │  └───────────┘   +-----------------+
                                      │                               │
                                      │   ┌────────────────────────┐  │
                                      │   │ Offset/Bookmark Store  │  │
                                      │   └────────────────────────┘  │
                                      └───────────────────────────────┘
                                                  |
                                                  v
                                          +----------------+
                                          | Prometheus /   |
                                          | Grafana        |
                                          +----------------+
```

1.  **Source**: Connects to the Canton participant's Transaction Service gRPC endpoint.
2.  **Stream**: Reads the stream of committed transactions visible to the configured party.
3.  **Filter/Transform**: Deserializes events and applies user-defined filters (e.g., only include `Asset:Transfer` events).
4.  **Serialize & Sink**: Encodes the event into the desired format (e.g., JSON) and publishes it to the configured sink (e.g., a Kafka topic).
5.  **Offset Management**: After successfully writing a batch of events, the connector checkpoints the ledger offset, ensuring it can resume from the correct position if it restarts.

## Quick Start (Local Development)

This guide walks through running the connector locally against a Canton sandbox and a Dockerized Kafka instance.

### Prerequisites

*   JDK 11 or higher
*   [sbt](https://www.scala-sbt.org/download.html) (Scala Build Tool)
*   [Docker](https://www.docker.com/get-started/) and Docker Compose
*   [DPM](https://docs.digitalasset.com/canton/stable/getting-started/quickstart-guide.html#install-the-canton-sdk) (Canton SDK)

### 1. Clone the Repository

```bash
git clone https://github.com/digital-asset/canton-event-streaming.git
cd canton-event-streaming
```

### 2. Build the Connector

This command compiles the source and packages it into a single executable "fat" JAR.

```bash
sbt assembly
```
The output will be located at `target/scala-2.13/canton-event-streaming-assembly-0.1.0.jar`.

### 3. Start Canton and Kafka

We'll use `docker-compose` to run Kafka and `dpm` to run a local Canton ledger.

First, create a `docker-compose.yml` for Kafka:
```yaml
# docker-compose.yml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.3
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.3
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

Now, start the services:
```bash
# In one terminal, start Kafka
docker-compose up -d

# In another terminal, start the Canton sandbox ledger
dpm sandbox
```

### 4. Configure the Connector

Create a configuration file named `local.conf` with the following content. This tells the connector to connect to the local sandbox and Kafka, and to stream events for the party `Alice`.

```hocon
# local.conf
canton-connector {
  name = "local-dev-connector"

  # Source Canton participant node
  source {
    type = "canton"
    host = "localhost"
    port = 6866
    party = "Alice" // The party whose transaction view we want to stream
    tls.enabled = false
  }

  # Destination sink
  sink {
    type = "kafka"
    kafka {
      bootstrap-servers = "localhost:9092"
      topic = "canton-alice-events"
      producer-properties {
        "acks" = "all"
        "client.id" = "canton-connector-alice"
      }
    }
  }

  # Offset store for resumability
  offset-store {
    type = "file"
    file.path = "/tmp/canton-connector-offset.txt"
  }

  # Monitor via Prometheus
  monitoring {
    prometheus.port = 9095
  }
}
```

### 5. Run the Connector

Execute the JAR file, passing the path to your configuration file.

```bash
java -Dconfig.file=local.conf -jar target/scala-2.13/canton-event-streaming-assembly-*.jar
```

You should see log output indicating a successful connection to both Canton and Kafka.

### 6. Test the Pipeline

Now, let's generate some activity on the ledger and watch it appear in Kafka.

First, set up a Kafka consumer in a new terminal to watch the topic:
```bash
# requires kafkacat (https://github.com/edenhill/kcat)
# On macOS: brew install kafkacat
kafkacat -b localhost:9092 -t canton-alice-events -C -J
```

Next, create a simple Daml model and script to create a contract.

**`daml/Test.daml`**:
```daml
module Test where

import Daml.Script

template Dummy
  with
    owner: Party
    data: Text
  where
    signatory owner

setup : Script ()
setup = do
  alice <- allocateParty "Alice"
  submit alice do
    createCmd Dummy with owner = alice, data = "hello world 1"
  submit alice do
    createCmd Dummy with owner = alice, data = "hello world 2"
  return ()
```

Run the script against the sandbox:
```bash
dpm script --dar .daml/dist/project-name-0.1.0.dar --script-name Test:setup --ledger-host localhost --ledger-port 6865
```

Within seconds, you will see JSON payloads appear in your `kafkacat` terminal, representing the `Dummy` contract creation events!

```json
{
  "event_type": "create",
  "contract_id": "00c8b6...",
  "template_id": "Main:Dummy",
  "payload": {
    "owner": "Alice::1220...",
    "data": "hello world 1"
  },
  "signatories": ["Alice::1220..."],
  "observers": [],
  "transaction_id": "tr-...",
  "offset": "000000..."
}
```

## Configuration

The connector is configured using a HOCON file (see `local.conf` above). The main sections are:

| Section                 | Description                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------------- |
| `canton-connector.name` | A unique name for this connector instance, used in logging and metrics.                                 |
| `canton-connector.source`   | Configures the connection to the Canton participant node, including host, port, party, and TLS settings. |
| `canton-connector.sink`     | Configures the destination. Set `type` to `kafka`, `pulsar`, or `kinesis` and provide nested settings. |
| `canton-connector.filter`   | (Optional) Rules to include/exclude events by template ID or interface ID.                              |
| `canton-connector.offset-store` | Configures where to persist the last-read ledger offset. Use `file` for local dev or `jdbc` for prod. |
| `canton-connector.monitoring` | Configures the Prometheus metrics endpoint.                                                             |
| `canton-connector.dlq`        | (Optional) Configures a sink (e.g., another Kafka topic) for events that fail processing.             |

For a full list of configuration options, please see the `reference.conf` file in the source code.

## Deployment

For production, we recommend deploying the connector as a containerized service (e.g., on Kubernetes).

### Docker Image

A `Dockerfile` is provided in the repository to build a container image.

```dockerfile
# Dockerfile
FROM eclipse-temurin:11-jre

WORKDIR /app

# Copy the assembly JAR from the build stage
COPY target/scala-2.13/canton-event-streaming-assembly-*.jar /app/connector.jar

# Expose ports for health checks and metrics
EXPOSE 8080 9095

ENTRYPOINT ["java", "-Dconfig.file=/app/config/application.conf", "-jar", "/app/connector.jar"]
```

You would build and push this image to your container registry.

### Kubernetes

A typical Kubernetes deployment would involve:
1.  A `ConfigMap` to mount the `application.conf` file.
2.  A `Secret` for sensitive information like TLS certificates and database passwords.
3.  A `Deployment` to manage the connector pods.
4.  A `Service` to expose the metrics port to your Prometheus scraper.

We recommend running at least two replicas of the connector in an active-passive configuration for high availability.

## Monitoring

The connector exposes a Prometheus endpoint on the port configured under `monitoring.prometheus.port`. Key metrics to monitor include:

*   `canton_connector_ledger_offset_lag`: The difference between the latest ledger offset and the offset processed by the connector. A growing lag indicates the connector is falling behind.
*   `canton_connector_events_processed_total`: A counter of events successfully sent to the sink.
*   `canton_connector_events_failed_total`: A counter of events that failed processing and were sent to the DLQ.
*   `canton_connector_sink_publish_duration_seconds`: A histogram of the latency to publish events to the sink.

We recommend setting up alerts on the offset lag and the failure rate.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

### Development Setup

1.  Clone the repository.
2.  Run `sbt` to enter the interactive build console.
3.  Use `compile` to compile the code and `test` to run the test suite.

Before submitting a pull request, please ensure all tests pass and that the code is formatted with `sbt scalafmtAll`.

## License

This project is licensed under the [Apache License 2.0](LICENSE).