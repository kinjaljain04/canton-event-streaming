# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Support for Apache Pekko Connectors as an alternative streaming backend.
- Option to publish Daml contract keys alongside contract create events.

### Changed
- Upgraded Canton SDK dependency to `3.4.0`.
- Migrated from Ledger API v1 endpoints to v2 endpoints for state streaming.

## [0.2.0] - 2024-09-15

### Added
- **Pulsar Schema Registry Support**: Added integration with Apache Pulsar's built-in schema registry for Avro schemas.
- **Filtering by Party**: New configuration option `filter.party-ids` to stream events only for a specific set of parties.
- **Filtering by Template**: New configuration option `filter.template-ids` to stream events only for a specific set of Daml templates.
- **AWS IAM Role Authentication**: Support for authenticating with AWS Kinesis using IAM roles for EC2/EKS.
- **Health Check Endpoint**: Added a `/health` endpoint that provides detailed status of the ledger connection and stream producer.

### Fixed
- Improved reconnection logic for intermittent Canton participant node downtime. The connector now uses an exponential backoff strategy.
- Resolved an issue where large transactions could exceed the default gRPC message size limit. The limit is now configurable.

## [0.1.0] - 2024-07-29

### Added
- **Initial Release** of the Canton Event Streaming connector.
- **Canton Ledger API Integration**: Connects to a Canton participant node via its gRPC Ledger API to stream transactions.
- **Streaming Backends**: Support for publishing events to:
  - Apache Kafka
  - Apache Pulsar
  - AWS Kinesis
- **Event Sources**: Configurable event source to stream:
  - Flat Transactions (`/v2/state/active-contracts`)
  - Transaction Trees (`/v2/stream/transactions`)
- **Reliable Delivery**: At-least-once delivery semantics with persistent offset management. Offsets can be stored locally on disk or in a dedicated Kafka topic.
- **Replay Functionality**: Ability to start streaming from the ledger beginning, a specific offset, or a specific timestamp.
- **Backpressure Handling**: Built-in stream backpressure to avoid overwhelming downstream systems or the connector itself.
- **Schema Registry Integration**: Automatic Avro schema generation from Daml templates and integration with Confluent Schema Registry for Kafka.
- **Configuration**: All settings managed through a single HOCON configuration file (`application.conf`).
- **Monitoring**: Exposes Prometheus metrics for throughput, latency, ledger connection status, and errors.
- **Containerization**: Dockerfile and instructions for building and running the connector as a Docker container.
- **Documentation**: Initial project `README.md` and `docs/CONFIGURATION.md`.