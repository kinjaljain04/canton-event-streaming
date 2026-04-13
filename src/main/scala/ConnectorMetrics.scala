package com.digitalasset.canton.connector

import io.prometheus.client.{CollectorRegistry, Counter, Gauge, Histogram}
import io.prometheus.client.hotspot.DefaultExports

/**
 * Singleton object for managing and exposing Prometheus metrics for the Canton Event Streaming Connector.
 *
 * This object centralizes all metric definitions, making them easily accessible from different parts of the application.
 * It follows Prometheus best practices for naming and labeling.
 *
 * To expose these metrics, an HTTP server can be started from the main application, e.g.:
 * `val server = new io.prometheus.client.exporter.HTTPServer(9090)`
 *
 * It is highly recommended to initialize the hotspot metrics for JVM monitoring as well:
 * `ConnectorMetrics.initializeJvmMetrics()`
 */
object ConnectorMetrics {

  /**
   * The central registry for all metrics defined in this object.
   * While the default registry is often used, creating a specific one can help with isolation,
   * especially in testing or complex deployment scenarios.
   */
  val registry: CollectorRegistry = new CollectorRegistry()

  /**
   * Initializes standard JVM metrics, providing insights into memory usage, garbage collection,
   * thread counts, etc. This should be called once at application startup.
   */
  def initializeJvmMetrics(): Unit = {
    DefaultExports.register(registry)
  }

  val eventsProcessed: Counter = Counter.build()
    .name("canton_connector_events_processed_total")
    .help("Total number of ledger events successfully processed and sent downstream.")
    .labelNames("event_type", "template_id", "destination") // event_type: create/archive, destination: kafka_topic/pulsar_topic etc.
    .register(registry)

  val batchesProcessed: Counter = Counter.build()
    .name("canton_connector_batches_processed_total")
    .help("Total number of event batches processed from the ledger stream.")
    .labelNames("destination_type") // e.g., kafka, pulsar, kinesis
    .register(registry)

  val processingErrors: Counter = Counter.build()
    .name("canton_connector_processing_errors_total")
    .help("Total number of errors encountered during event processing.")
    // stage: source (reading from ledger), transform (filtering/mapping), sink (writing to destination)
    // error_type: serialization, connection, validation, etc.
    .labelNames("stage", "error_type")
    .register(registry)

  val lastLedgerOffset: Gauge = Gauge.build()
    .name("canton_connector_last_ledger_offset_processed")
    .help("The last ledger offset successfully processed. Note: This is only useful for numeric offsets.")
    .labelNames("participant_id", "subscription_id")
    .register(registry)

  val processingLagSeconds: Histogram = Histogram.build()
    .name("canton_connector_processing_lag_seconds")
    .help("Histogram of the time lag between an event's record time on the ledger and its processing time by the connector.")
    // Buckets from 100ms to 5 minutes
    .buckets(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0)
    .register(registry)

  val downstreamPublishDurationSeconds: Histogram = Histogram.build()
    .name("canton_connector_downstream_publish_duration_seconds")
    .help("Histogram of the time taken to publish a batch of events to the downstream system (e.g., Kafka, Pulsar).")
    .labelNames("destination_type") // "kafka", "pulsar", "kinesis"
    .register(registry)

  val deadLetterQueueDepth: Gauge = Gauge.build()
    .name("canton_connector_dlq_depth_total")
    .help("Current number of events in the Dead Letter Queue.")
    .register(registry)

  val eventsInFlight: Gauge = Gauge.build()
    .name("canton_connector_events_in_flight")
    .help("Current number of events being processed but not yet acknowledged downstream (a key backpressure indicator).")
    .labelNames("destination_type")
    .register(registry)

  val connectionStatus: Gauge = Gauge.build()
    .name("canton_connector_connection_status")
    .help("Status of connections to external systems. 1 for connected, 0 for disconnected.")
    // system: canton_ledger, kafka_broker, schema_registry, etc.
    .labelNames("system")
    .register(registry)

  val reconnectsTotal: Counter = Counter.build()
    .name("canton_connector_reconnects_total")
    .help("Total number of times the connector has had to reconnect to a system.")
    .labelNames("system")
    .register(registry)

  /**
   * Helper method to update the last processed offset gauge.
   * This method attempts to parse a string offset into a double, which is required by the Gauge.
   * It is only effective for ledger implementations with numeric or lexicographically-sortable-and-convertible offsets.
   * For fully opaque string offsets, this metric will not be meaningful.
   *
   * @param offset The string representation of the last successfully processed offset.
   * @param participantId The identifier of the participant node being subscribed to.
   * @param subscriptionId A unique identifier for the specific event subscription/stream.
   */
  def updateLastOffset(offset: String, participantId: String, subscriptionId: String): Unit = {
    try {
      // A simple attempt to convert offset to a number. This may need to be adapted
      // for specific ledger offset formats (e.g., by taking a hash or parsing a prefix).
      val numericOffset = offset.filter(c => c.isDigit || c == '.').toDouble
      lastLedgerOffset.labels(participantId, subscriptionId).set(numericOffset)
    } catch {
      case _: NumberFormatException =>
      // Silently fail if offset is not numeric. The application should log this at a WARN level if offsets are expected to be numeric.
      // An alternative for non-numeric offsets is to expose them via an 'info' metric, but that is less standard for progress tracking.
    }
  }
}