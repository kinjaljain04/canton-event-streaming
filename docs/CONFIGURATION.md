# Canton Event Streaming Connector: Configuration Reference

This document provides a comprehensive reference for all configuration options available in the Canton Event Streaming Connector. The configuration is specified in a file using the [HOCON (Human-Optimized Config Object Notation)](https://github.com/lightbend/config/blob/main/HOCON.md) format, typically named `connector.conf`.

## Table of Contents

- [Core Connector Configuration](#core-connector-configuration)
  - [Canton Ledger Connection (`canton.ledger`)](#canton-ledger-connection-cantonledger)
  - [Connector Settings (`connector`)](#connector-settings-connector)
- [Sink Configuration (`sink`)](#sink-configuration-sink)
  - [Kafka Sink (`sink.kafka`)](#kafka-sink-sinkkafka)
  - [Pulsar Sink (`sink.pulsar`)](#pulsar-sink-sinkpulsar)
  - [AWS Kinesis Sink (`sink.kinesis`)](#aws-kinesis-sink-sinkkinesis)
- [Data Format (`format`)](#data-format-format)
- [Metrics (`metrics`)](#metrics-metrics)
- [Full Example Configuration](#full-example-configuration)

---

## Core Connector Configuration

This section covers the essential settings for connecting to the Canton ledger and controlling the behavior of the connector itself.

### Canton Ledger Connection (`canton.ledger`)

This block configures the connection to the Canton participant's Ledger API.

```hocon
canton.ledger {
  # Hostname or IP address of the Canton participant's Ledger API gRPC server.
  host = "localhost"

  # Port of the Ledger API gRPC server.
  port = 6866

  # TLS configuration for a secure connection to the Ledger API.
  tls {
    enabled = false
    # Path to the CA certificate file (.crt) for the participant node.
    # Required if tls.enabled = true.
    # trust-cert-collection-file = "/path/to/participant/tls/ca.crt"
  }

  # Authentication settings.
  authentication {
    # The only supported type is "jwt".
    type = "jwt"

    # Path to a file containing the JWT token for authenticating with the Ledger API.
    # Using a file is recommended for security over pasting the token here directly.
    jwt-token-file = "/path/to/ledger-api.token"
  }
}
```

| Parameter                             | Type    | Description                                                                                             | Default     |
| ------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------- | ----------- |
| `host`                                | String  | Hostname or IP of the Ledger API server.                                                                | `localhost` |
| `port`                                | Integer | Port of the Ledger API server.                                                                          | `6866`      |
| `tls.enabled`                         | Boolean | Set to `true` to enable TLS encryption.                                                                 | `false`     |
| `tls.trust-cert-collection-file`      | String  | Filesystem path to the trusted CA certificate file. **Required** if `tls.enabled` is `true`.              | (none)      |
| `authentication.type`                 | String  | The authentication method. Currently, only `"jwt"` is supported.                                        | `"jwt"`     |
| `authentication.jwt-token-file`       | String  | Filesystem path to a file containing the raw JWT bearer token. **Required**.                              | (none)      |

### Connector Settings (`connector`)

This block defines the core behavior of the streaming connector.

```hocon
connector {
  # A unique name for this connector instance. Used in logging and metrics.
  name = "canton-to-kafka-connector-01"

  # A list of Canton party IDs for which to stream transactions.
  party-ids = ["TradingParty::1220...", "SettlementParty::1220..."]

  # Configuration for the starting point of the stream.
  begin-offset {
    # Mode can be "earliest", "latest", or "explicit".
    # - earliest: Start from the very beginning of the ledger.
    # - latest: Start from the current end of the ledger (new events only).
    # - explicit: Start from a specific ledger offset.
    mode = "earliest"

    # The specific ledger offset string. Required only if mode is "explicit".
    # explicit-value = "00000000000000060d62..."
  }

  # Optional configuration for the end point of the stream.
  end-offset {
    # Mode can be "unbounded" or "explicit".
    # - unbounded: Stream indefinitely.
    # - explicit: Stop streaming after reaching a specific offset.
    mode = "unbounded"

    # The specific ledger offset string. Required only if mode is "explicit".
    # explicit-value = "0000000000000009af01..."
  }

  # Configuration for persisting the last processed ledger offset.
  offset-persistence {
    # Type can be "file" or "memory".
    # "kafka" is also available if using the Kafka sink for topic-based storage.
    type = "file"

    # Path to the offset storage file. Required if type is "file".
    file.path = "/var/data/connector/offsets.dat"
  }

  # Backpressure settings to prevent overwhelming the sink.
  backpressure {
    # Maximum number of concurrent requests to the Ledger API.
    max-inflight-requests = 64

    # Size of the internal buffer for events before applying backpressure.
    buffer-size = 1024
  }
}
```

| Parameter                           | Type         | Description                                                                                       | Default      |
| ----------------------------------- | ------------ | ------------------------------------------------------------------------------------------------- | ------------ |
| `name`                              | String       | A unique identifier for the connector.                                                            | (none)       |
| `party-ids`                         | List[String] | A list of parties to stream data for. **Required**.                                               | `[]`         |
| `begin-offset.mode`                 | String       | Where to start streaming: `earliest`, `latest`, `explicit`.                                       | `latest`     |
| `begin-offset.explicit-value`       | String       | The ledger offset string if `mode` is `explicit`.                                                 | (none)       |
| `end-offset.mode`                   | String       | When to stop streaming: `unbounded`, `explicit`.                                                  | `unbounded`  |
| `end-offset.explicit-value`         | String       | The ledger offset string if `mode` is `explicit`.                                                 | (none)       |
| `offset-persistence.type`           | String       | How to store progress: `file`, `memory`.                                                          | `memory`     |
| `offset-persistence.file.path`      | String       | Path to the offset file if `type` is `file`.                                                      | (none)       |
| `backpressure.max-inflight-requests`| Integer      | Max concurrent gRPC requests to the ledger.                                                       | `64`         |
| `backpressure.buffer-size`          | Integer      | Internal buffer size for ledger events.                                                           | `1024`       |

---

## Sink Configuration (`sink`)

This section defines the destination for the ledger event stream. You must specify a `type` and configure the corresponding block.

### Kafka Sink (`sink.kafka`)

For streaming events to an Apache Kafka cluster.

```hocon
sink {
  type = "kafka"

  kafka {
    # Comma-separated list of Kafka broker host:port pairs.
    bootstrap-servers = "kafka1:9092,kafka2:9092"

    # The target Kafka topic to publish events to.
    topic = "canton-ledger-events"

    # Exactly-once delivery semantics (EOS). Requires Kafka 0.11+.
    # If true, a transactional.id will be generated based on connector.name and topic.
    enable-idempotence = true

    # A map of standard Kafka producer properties.
    # See: https://kafka.apache.org/documentation/#producerconfigs
    properties {
      acks = "all"
      compression.type = "zstd"
      # Example for SSL/TLS encryption
      # security.protocol = "SSL"
      # ssl.truststore.location = "/path/to/kafka.client.truststore.jks"
      # ssl.truststore.password = "secret"
    }

    # Optional integration with a Confluent-compatible Schema Registry.
    # Only used if format.type is "avro".
    schema-registry {
      enabled = false
      # URL of the Schema Registry.
      # url = "http://schema-registry:8081"
      # properties {
      #   basic.auth.credentials.source = "USER_INFO"
      #   basic.auth.user.info = "user:password"
      # }
    }
  }
}
```

### Pulsar Sink (`sink.pulsar`)

For streaming events to an Apache Pulsar cluster.

```hocon
sink {
  type = "pulsar"

  pulsar {
    # The Pulsar broker service URL.
    service-url = "pulsar+ssl://pulsar.my-company.com:6651"

    # The target Pulsar topic.
    topic = "persistent://public/default/canton-ledger-events"

    # Pulsar client authentication settings.
    authentication {
      # Example for token-based authentication.
      plugin-class-name = "org.apache.pulsar.client.impl.auth.AuthenticationToken"
      params = "token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
    }

    # Pulsar client TLS settings.
    tls {
      trust-certs-file-path = "/path/to/pulsar/ca.crt"
      allow-insecure-connection = false
      enable-hostname-verification = true
    }

    # Pulsar producer settings.
    producer {
      name = "canton-connector-producer"
      send-timeout-ms = 30000
      batching-enabled = true
      batching-max-publish-delay-ms = 10
    }
  }
}
```

### AWS Kinesis Sink (`sink.kinesis`)

For streaming events to an Amazon Kinesis Data Stream.

```hocon
sink {
  type = "kinesis"

  kinesis {
    # The name of the Kinesis Data Stream.
    stream-name = "canton-ledger-stream"

    # The AWS region where the stream is located.
    region = "us-east-1"

    # Configuration for AWS credentials.
    credentials {
      # Provider can be "default", "profile", or "static".
      # - default: Uses the default AWS credential provider chain.
      # - profile: Uses a named profile from ~/.aws/credentials.
      # - static: Uses explicitly provided keys.
      provider = "default"

      # Required if provider is "profile".
      # profile-name = "my-aws-profile"

      # Required if provider is "static".
      # access-key-id = "AKIA..."
      # secret-access-key = "..."
    }

    # Which field from the Canton event payload to use as the Kinesis partition key.
    # Using "transaction_id" ensures events from the same transaction go to the same shard.
    # Using "contract_id" (for create events) partitions by contract.
    partition-key-field = "transaction_id"

    # Enables KPL-style record aggregation to increase throughput and reduce costs.
    aggregation-enabled = true
  }
}
```

---

## Data Format (`format`)

Configure the serialization format for event payloads sent to the sink.

```hocon
format {
  # The serialization format. Supported values: "json", "avro".
  type = "json"

  # JSON-specific settings.
  json {
    # If true, JSON output will be indented for human readability.
    # Recommended to be false in production.
    pretty-print = false
  }

  # Avro-specific settings.
  # avro {
  #   # Schema management strategy.
  #   # - subject-name-strategy: "TopicNameStrategy", "RecordNameStrategy", etc.
  # }
}
```

---

## Metrics (`metrics`)

Configure observability and monitoring for the connector.

```hocon
metrics {
  # Enable or disable the metrics endpoint.
  enabled = true

  # The metrics provider. Supported values: "prometheus".
  provider = "prometheus"

  prometheus {
    # The host to bind the Prometheus exporter HTTP server to.
    host = "0.0.0.0"

    # The port for the Prometheus exporter HTTP server.
    port = 9095
  }
}
```

---

## Full Example Configuration

This example shows a complete `connector.conf` file configured to stream events from a TLS-enabled Canton participant to a Kafka cluster, using file-based offset storage and Prometheus metrics.

```hocon
# Core Canton Ledger Connection
canton.ledger {
  host = "participant1.canton.network"
  port = 6866
  tls {
    enabled = true
    trust-cert-collection-file = "/etc/canton-connector/tls/ca.crt"
  }
  authentication {
    type = "jwt"
    jwt-token-file = "/etc/canton-connector/secrets/ledger-api.token"
  }
}

# Core Connector Settings
connector {
  name = "canton-fx-settlement-stream"
  party-ids = ["FXDealer::1220...", "CentralBank::1220..."]
  begin-offset {
    mode = "earliest"
  }
  offset-persistence {
    type = "file"
    file.path = "/var/data/canton-connector/offsets.json"
  }
  backpressure {
    max-inflight-requests = 128
    buffer-size = 2048
  }
}

# Sink Configuration (Kafka)
sink {
  type = "kafka"
  kafka {
    bootstrap-servers = "kafka-broker1:9092,kafka-broker2:9092"
    topic = "canton-fx-settlement-events"
    enable-idempotence = true
    properties {
      acks = "all"
      compression.type = "snappy"
      delivery.timeout.ms = 120000
      security.protocol = "SASL_SSL"
      sasl.mechanism = "SCRAM-SHA-512"
      sasl.jaas.config = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"connector_user\" password=\"secret_password\";"
      ssl.truststore.location = "/etc/canton-connector/tls/kafka.truststore.jks"
      ssl.truststore.password = "truststore_secret"
    }
  }
}

# Data Format
format {
  type = "json"
  json {
    pretty-print = false
  }
}

# Metrics
metrics {
  enabled = true
  provider = "prometheus"
  prometheus {
    host = "0.0.0.0"
    port = 9095
  }
}
```