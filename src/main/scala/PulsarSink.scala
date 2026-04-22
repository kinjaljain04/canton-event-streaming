package com.digitalasset.canton.connector

import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.Done
import org.apache.pulsar.client.api._

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._
import scala.util.control.NonFatal

/**
 * Configuration for the PulsarSink.
 *
 * @param serviceUrl The connection URL for the Pulsar cluster (e.g., "pulsar://localhost:6650").
 * @param topic The Pulsar topic to which events will be published.
 * @param producerName An optional name for the producer. Specifying a unique, stable name is
 *                     **required** to enable Pulsar's server-side message deduplication,
 *                     which is key to achieving exactly-once semantics.
 * @param sendTimeoutMs The timeout for sending a message.
 * @param blockIfQueueFull If true, the `send` and `sendAsync` methods will block if the outgoing
 *                         message queue is full. This is a key mechanism for backpressure.
 * @param batchingEnabled Whether to enable message batching on the producer.
 * @param batchingMaxMessages The maximum number of messages to allow in a batch.
 * @param batchingMaxPublishDelayMicros The maximum time to wait before sending a batch.
 */
final case class PulsarConfig(
    serviceUrl: String,
    topic: String,
    producerName: Option[String],
    sendTimeoutMs: Long = 30000L,
    blockIfQueueFull: Boolean = true,
    batchingEnabled: Boolean = true,
    batchingMaxMessages: Int = 1000,
    batchingMaxPublishDelayMicros: Long = 1000L
)

/**
 * A Pulsar producer wrapper that handles client/producer lifecycle and asynchronous message sending.
 * Implements AutoCloseable to be used with resource management constructs.
 *
 * This class is designed to be thread-safe and can be shared across multiple streams.
 * It assumes a `LedgerEvent` type with `offset`, `key`, and `value` properties is defined elsewhere.
 */
class PulsarWriter(config: PulsarConfig)(implicit ec: ExecutionContext)
    extends AutoCloseable
    with LazyLogging {

  private val client: PulsarClient = PulsarClient
    .builder()
    .serviceUrl(config.serviceUrl)
    // Note: In a production environment, you would configure authentication, e.g.:
    // .authentication(AuthenticationFactory.token("..."))
    .build()

  private val producer: Producer[String] = {
    val builder: ProducerBuilder[String] = client
      .newProducer(Schema.STRING)
      .topic(config.topic)
      .sendTimeout(config.sendTimeoutMs.toInt, TimeUnit.MILLISECONDS)
      .blockIfQueueFull(config.blockIfQueueFull)

    if (config.batchingEnabled) {
      builder
        .enableBatching(true)
        .batchingMaxMessages(config.batchingMaxMessages)
        .batchingMaxPublishDelay(config.batchingMaxPublishDelayMicros, TimeUnit.MICROSECONDS)
    } else {
      builder.enableBatching(false)
    }

    // Producer name is crucial for message deduplication (exactly-once semantics).
    // When a producer name is set, sequenceId must be used for each message.
    config.producerName.foreach { name =>
      logger.info(s"Enabling Pulsar message deduplication with producer name: $name")
      builder.producerName(name)
    }

    logger.info(s"Creating Pulsar producer for topic '${config.topic}'...")
    val p = builder.create()
    logger.info(s"Pulsar producer for topic '${config.topic}' created successfully.")
    p
  }

  /**
   * Asynchronously sends a single LedgerEvent to the configured Pulsar topic.
   *
   * @param event The LedgerEvent to send. It must have an `offset` (for sequenceId),
   *              `key` (for partitioning), and `value` (the payload).
   * @return A Future that completes with the MessageId when the message is acknowledged by Pulsar,
   *         or fails if sending is unsuccessful.
   */
  def write[E <: { def offset: Long; def key: String; def value: String }](
      event: E
  ): Future[MessageId] = {
    val messageBuilder: TypedMessageBuilder[String] = producer
      .newMessage()
      .key(event.key)
      .value(event.value)
      .eventTime(System.currentTimeMillis())

    // The ledger offset is a monotonically increasing number, perfect for Pulsar's sequenceId.
    // This is required for message deduplication when a producer name is configured.
    if (config.producerName.isDefined) {
      messageBuilder.sequenceId(event.offset)
    }

    val sendFuture = messageBuilder.sendAsync().asScala

    sendFuture
      .map { msgId =>
        logger.trace(s"Successfully sent message for offset ${event.offset} with Pulsar message ID $msgId")
        msgId
      }
      .recoverWith { case NonFatal(e) =>
        logger.error(
          s"Failed to send message for offset ${event.offset} to Pulsar topic ${config.topic}",
          e
        )
        Future.failed(e) // Propagate failure to the stream
      }
  }

  override def close(): Unit = {
    logger.info(s"Closing Pulsar producer for topic '${config.topic}'...")
    try {
      producer.close()
    } catch {
      case NonFatal(e) => logger.error("Error closing Pulsar producer", e)
    }

    logger.info(s"Closing Pulsar client for service URL '${config.serviceUrl}'...")
    try {
      client.close()
    } catch {
      case NonFatal(e) => logger.error("Error closing Pulsar client", e)
    }
    logger.info("Pulsar resources closed.")
  }
}

/**
 * Factory for creating a Pekko Streams Sink that publishes events to Apache Pulsar.
 */
object PulsarSink extends LazyLogging {

  /**
   * Creates a Pekko Streams Sink that sends events to a Pulsar topic.
   *
   * The lifecycle of the underlying PulsarWriter is NOT managed by this Sink. It is the
   * responsibility of the caller to create the PulsarWriter before the stream starts and
   * close it after the stream terminates.
   *
   * @tparam E The type of event being processed. Must conform to a structure with
   *           `offset: Long`, `key: String`, and `value: String`.
   * @param writer The PulsarWriter instance to use for publishing.
   * @param parallelism The number of concurrent messages to send to Pulsar. Controls backpressure.
   * @return A `Sink[E, Future[Done]]` that can be used in a Pekko Streams graph.
   */
  def apply[E <: { def offset: Long; def key: String; def value: String }](
      writer: PulsarWriter,
      parallelism: Int
  ): Sink[E, Future[Done]] = {
    require(parallelism > 0, "Parallelism must be positive.")

    Sink.foreachAsync[E](parallelism) { event =>
      writer.write(event).map(_ => ()) // Discard MessageId, return Future[Unit] for the Sink
    }
  }
}