# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Experimental support for Google Cloud Pub/Sub sink.

### Changed
- Upgraded Canton SDK dependency to 3.4.0.
- Migrated Ledger API client from `v1` to `v2/state` endpoints for improved performance and streaming semantics.

## [1.2.0] - 2024-07-15

### Added
- **AWS Kinesis Sink:** Added a new sink for streaming ledger events directly to Amazon Kinesis Data Streams.
- **Schema Registry Integration:** Support for Confluent Schema Registry. Contract schemas can be automatically registered and evolved, enabling robust data contracts for downstream consumers.
- **Prometheus Metrics:** Exposed key operational metrics (e.g., events processed, offset lag, connection status) via a `/metrics` endpoint for Prometheus scraping.
- **Transaction Trees:** Added a configuration option (`output.transactionTrees`) to stream full transaction trees instead of just create/archive events.

### Changed
- Improved backpressure handling for all sinks to prevent out-of-memory errors under high load.
- Replaced HOCON configuration with a more standard YAML format (`connector.yaml`). A migration guide has been added to the documentation.

### Fixed
- Correctly handle ledger API token expiry and refresh without interrupting the stream.

## [1.1.1] - 2024-06-20

### Fixed
- **Offset Management:** Fixed a critical bug where offsets were not always committed atomically, potentially leading to duplicate events on restart after a crash. The connector now ensures exactly-once semantics for Kafka and Pulsar sinks.
- **Pulsar Sink:** Resolved an issue where the Pulsar client would not gracefully reconnect after a transient network partition.

## [1.1.0] - 2024-06-12

### Added
- **Replay Functionality:** New configuration `source.startFrom` allows replaying the ledger from a specific offset or from the beginning.
- **Event Filtering:** Added `source.filter.templateIds` to specify a list of template IDs to stream, reducing traffic for consumers interested in a subset of contracts.
- **Header Injection:** Support for injecting custom static headers into Kafka/Pulsar messages via the `sink.headers` configuration.

### Changed
- Enhanced logging to include transaction IDs and event IDs for better traceability.
- Refactored sink implementations into a common interface to simplify adding new streaming backends.

## [1.0.0] - 2024-05-28

### Added
- Initial public release of the Canton Event Streaming Connector.
- Support for Canton Ledger API v1 endpoints (`/v1/stream/transactions`).
- **Kafka Sink:** Fully-featured sink for Apache Kafka with configurable topic mapping and partitioning strategies.
- **Pulsar Sink:** Sink for Apache Pulsar.
- Comprehensive documentation including quickstart guides and configuration references.
- Docker image for easy deployment.