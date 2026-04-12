# Canton Event Streaming Connector Configuration

This document provides a comprehensive reference for all configuration parameters for the Canton Event Streaming Connector.

## Overview

Configuration is primarily managed through **environment variables**. This approach is container-friendly and aligns with modern deployment practices (e.g., Docker, Kubernetes).

All environment variables are prefixed with `CES_` (Canton Event Streamer) to avoid conflicts.

---

## 1. Common Configuration

These settings are required regardless of the chosen sink (Kafka, Pulsar, or Kinesis). They configure the connection to the Canton participant, authentication, and event filtering.

| Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `CES_CANTON_HOST` | The gRPC hostname of the Canton participant node. | Yes | |
| `CES_CANTON_PORT` | The gRPC port of the Canton participant node. | Yes | |
| `CES_CANTON_PARTY_IDS` | A comma-separated list of party identifiers to stream events for. The connector will subscribe to the transaction stream for all specified parties. | Yes | |
| `CES_CANTON_TLS_ENABLED` | Set to `true` to enable TLS for the gRPC connection to the Canton participant. | No | `false` |
| `CES_CANTON_TLS_CA_CERT_PATH` | Path to the root CA certificate file. Required if `CES_CANTON_TLS_ENABLED` is `true` and the participant uses a custom certificate authority. | No | |
| `CES_CANTON_AUTH_TOKEN_FILE` | Path to a file containing the JWT for authenticating with the Canton participant's Ledger API. | No | |
| `CES_CANTON_TEMPLATE_IDS` | An optional comma-separated list of fully qualified template IDs to filter for (e.g., `MyModule:MyTemplate,Another.Module:AnotherTemplate`). If omitted, all events for the specified parties are streamed. | No | |
| `CES_CANTON_START_OFFSET` | Defines where to begin streaming from the ledger. Can be `beginning`, `end`, or a specific ledger offset string. | No | `end` |
| `CES_SINK_TYPE` | The destination event streaming platform. Must be one of `KAFKA`, `PULSAR`, or `KINESIS`. | Yes | |
| `CES_LOG_LEVEL` | The logging level for the connector. Supported values: `DEBUG`, `INFO`, `WARN`, `ERROR`. | No | `INFO` |

---

## 2. Sink-Specific Configuration

Configure the section corresponding to the `CES_SINK_TYPE` you have selected.

### 2.1 Apache Kafka (`CES_SINK_TYPE=KAFKA`)

| Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `CES_KAFKA_BOOTSTRAP_SERVERS` | A comma-separated list of Kafka broker host:port pairs. | Yes | |
| `CES_KAFKA_TOPIC` | The target Kafka topic where Canton events will be published. | Yes | |
| `CES_KAFKA_CLIENT_ID` | An identifier for the Kafka producer client. | No | `canton-event-streamer` |
| `CES_KAFKA_ACKS` | The number of acknowledgments the producer requires. Recommended for durability: `all` (-1). | No | `all` |
| `CES_KAFKA_COMPRESSION_TYPE` | The compression type for all messages. Can be `none`, `gzip`, `snappy`, `lz4`, or `zstd`. | No | `snappy` |
| `CES_KAFKA_SECURITY_PROTOCOL` | Protocol used to communicate with brokers. `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. | No | `PLAINTEXT` |
| `CES_KAFKA_SASL_MECHANISM` | SASL mechanism to use for authentication. `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. | No | |
| `CES_KAFKA_SASL_JAAS_CONFIG` | The SASL JAAS configuration string. Example: `org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="password";` | No | |
| `CES_KAFKA_SSL_TRUSTSTORE_LOCATION` | Path to the truststore file. | No | |
| `CES_KAFKA_SSL_TRUSTSTORE_PASSWORD` | The password for the truststore file. | No | |

### 2.2 Apache Pulsar (`CES_SINK_TYPE=PULSAR`)

| Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `CES_PULSAR_SERVICE_URL` | The service URL for the Pulsar cluster (e.g., `pulsar://localhost:6650` or `pulsar+ssl://my-cluster:6651`). | Yes | |
| `CES_PULSAR_TOPIC` | The target Pulsar topic, including the tenant and namespace (e.g., `persistent://public/default/canton-events`). | Yes | |
| `CES_PULSAR_AUTHENTICATION_PLUGIN` | The fully qualified class name of the authentication plugin. Example: `org.apache.pulsar.client.impl.auth.AuthenticationToken`. | No | |
| `CES_PULSAR_AUTHENTICATION_PARAMS` | A string of parameters for the authentication plugin. Example for token auth: `token:eyJhbGciOi...`. | No | |
| `CES_PULSAR_TLS_ENABLED` | Set to `true` if the service URL uses `pulsar+ssl`. | No | `false` |
| `CES_PULSAR_TLS_TRUST_CERTS_FILE_PATH` | Path to the trusted TLS certificate file. | No | |
| `CES_PULSAR_BATCHING_ENABLED` | Enable or disable message batching by the producer. | No | `true` |
| `CES_PULSAR_BATCHING_MAX_MESSAGES` | The maximum number of messages to include in a single batch. | No | `1000` |

### 2.3 AWS Kinesis (`CES_SINK_TYPE=KINESIS`)

Authentication is handled via the standard AWS credential provider chain (environment variables, profile, IAM instance role).

| Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `CES_KINESIS_STREAM_NAME` | The name of the target Kinesis Data Stream. | Yes | |
| `CES_KINESIS_REGION` | The AWS region where the Kinesis stream is located (e.g., `us-east-1`). | Yes | |
| `CES_KINESIS_PARTITION_KEY_TEMPLATE`| A template to dynamically generate the partition key for each record. Use placeholders like `{transactionId}`, `{contractId}`, or `{templateId}`. A good key ensures even shard distribution. | Yes | `{transactionId}` |
| `CES_KINESIS_AGGREGATION_ENABLED` | If `true`, multiple user records are aggregated into a single Kinesis record to improve throughput and reduce cost. | No | `true` |
| `AWS_ACCESS_KEY_ID` | Your AWS access key ID. (Standard AWS env var). | Conditional | |
| `AWS_SECRET_ACCESS_KEY` | Your AWS secret access key. (Standard AWS env var). | Conditional | |
| `AWS_SESSION_TOKEN` | Your AWS session token. (Standard AWS env var). | No | |

**Note on AWS Auth:** For production environments, it is strongly recommended to use IAM roles for EC2 instances or EKS service accounts instead of setting `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` directly.

---

## 3. Data Format & Schema Registry

These settings control the serialization format of the event payload and its integration with a schema registry.

| Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `CES_FORMAT_TYPE` | The output data format for event payloads. Supported values: `JSON`, `AVRO`. | No | `JSON` |
| `CES_SCHEMA_REGISTRY_TYPE` | The type of schema registry to use. Supported: `CONFLUENT`, `AWS_GLUE`, `NONE`. Required if `CES_FORMAT_TYPE` is `AVRO`. | No | `NONE` |
| `CES_SCHEMA_REGISTRY_URL` | The URL of the Confluent Schema Registry. Required if `CES_SCHEMA_REGISTRY_TYPE` is `CONFLUENT`. | No | |
| `CES_SCHEMA_REGISTRY_AUTH_USER_INFO` | Basic authentication credentials for the schema registry in `user:password` format. | No | |
| `CES_SCHEMA_REGISTRY_AWS_REGION` | The AWS region for the AWS Glue Schema Registry. Required if `CES_SCHEMA_REGISTRY_TYPE` is `AWS_GLUE`. | No | |
| `CES_SCHEMA_AUTOREGISTRATION_ENABLED` | Set to `true` to allow the connector to automatically register new schemas derived from Daml templates. | No | `true` |
| `CES_SCHEMA_SUBJECT_NAME_STRATEGY` | For Kafka, defines how to map topics to schema subjects. Supported: `TopicNameStrategy`, `RecordNameStrategy`, `TopicRecordNameStrategy`. | No | `TopicNameStrategy` |

---

## 4. Resiliency and Performance

These parameters fine-tune the connector's behavior under load and during failure scenarios, managing backpressure, retries, and offset persistence.

| Variable | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `CES_OFFSET_STORAGE_TYPE` | How to persist the last processed ledger offset. `MEMORY` is for testing only. For production, use `FILE` or `KAFKA` (persists to a Kafka topic). | No | `MEMORY` |
| `CES_OFFSET_STORAGE_PATH` | If `CES_OFFSET_STORAGE_TYPE` is `FILE`, this is the path to the offset storage file. | Conditional | `./data/offsets.json` |
| `CES_OFFSET_STORAGE_KAFKA_TOPIC` | The Kafka topic for storing offsets if `CES_OFFSET_STORAGE_TYPE` is `KAFKA`. | Conditional | `_canton_streamer_offsets` |
| `CES_STREAM_COMMIT_INTERVAL_MS` | The maximum time in milliseconds between offset commits. | No | `5000` |
| `CES_STREAM_COMMIT_BATCH_SIZE` | The maximum number of records to process before committing their offset. An offset is committed when either this or the interval is reached. | No | `1000` |
| `CES_STREAM_BUFFER_SIZE` | The size of the in-memory buffer for events before they are sent to the sink. Acts as a backpressure mechanism. | No | `10000` |
| `CES_STREAM_RETRY_MAX_ATTEMPTS` | Maximum number of retry attempts for transient sink errors before the connector exits. `0` means no retries. `-1` means infinite retries. | No | `10` |
| `CES_STREAM_RETRY_BACKOFF_INITIAL_MS`| The initial backoff delay in milliseconds for retries, which increases exponentially. | No | `500` |
| `CES_STREAM_RETRY_BACKOFF_MAX_MS` | The maximum backoff delay in milliseconds between retries. | No | `30000` |