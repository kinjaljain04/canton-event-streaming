package com.digitalasset.canton.events

import com.typesafe.config.Config
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.{
  LeaderNotAvailableException,
  NetworkException,
  NotEnoughReplicasException,
  TimeoutException
}
import org.slf4j.LoggerFactory

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * A sink that sends Canton ledger events to an Apache Kafka topic.
 *
 * This implementation is resilient to transient broker unavailability. It uses an
 * exponential backoff retry mechanism for network-related failures when sending
 * messages.
 *
 * @param config The application configuration for this sink.
 */
class KafkaSink(config: Config) extends Sink {

  private val logger = LoggerFactory.getLogger(getClass)

  // Configuration for Kafka producer and retry logic
  private val kafkaConfig = config.getConfig("kafka")
  private val topic = kafkaConfig.getString("topic")
  private val bootstrapServers = kafkaConfig.getString("bootstrap-servers")
  private val producerProperties = kafkaConfig.getConfig("producer-properties")

  // Exponential backoff configuration
  private val retryConfig = kafkaConfig.getConfig("retry")
  private val maxAttempts = retryConfig.getInt("max-attempts")
  private val initialDelay = retryConfig.getDuration("initial-delay").toMillis.millis
  private val maxDelay = retryConfig.getDuration("max-delay").toMillis.millis
  private val backoffFactor = retryConfig.getDouble("factor")

  // A dedicated single-threaded scheduler for handling retries. This avoids blocking application threads.
  private val scheduler = Executors.newSingleThreadScheduledExecutor(
    (r: Runnable) => {
      val t = new Thread(r, "kafka-sink-retry-scheduler")
      t.setDaemon(true)
      t
    }
  )
  // Execution context for composing futures, using a global context is fine here.
  private implicit val ec: ExecutionContext = ExecutionContext.global

  // The KafkaProducer is lazy to avoid initialization during construction if not needed.
  // It's also thread-safe, so one instance can be shared.
  private lazy val producer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    // Sensible defaults for high-throughput, reliable producers.
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(Producer-Config.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // Ensures exactly-once in-order delivery per partition.

    // Overlay any user-defined properties from the config file.
    producerProperties.entrySet().asScala.foreach { entry =>
      props.put(entry.getKey, entry.getValue.unwrapped().toString)
    }
    logger.info(s"Initializing KafkaProducer for servers: $bootstrapServers")
    new KafkaProducer[String, String](props)
  }

  /**
   * Sends a single ledger event to the configured Kafka topic.
   *
   * This method initiates the send operation and will retry with exponential backoff
   * if the broker is temporarily unavailable.
   *
   * @param event The LedgerEvent to send.
   * @return A Future that completes when the message is successfully acknowledged by Kafka,
   *         or fails if all retry attempts are exhausted.
   */
  override def send(event: LedgerEvent): Future[Unit] = {
    val record = new ProducerRecord[String, String](topic, event.transactionId, event.payload)
    sendWithRetry(record, 1, initialDelay).map(_ => ())
  }

  /**
   * The core retry logic. It attempts to send a record and, on specific retriable failures,
   * schedules another attempt after a calculated delay.
   *
   * @param record The ProducerRecord to send.
   * @param attempt The current attempt number (starting from 1).
   * @param delay The delay to wait before this attempt.
   * @return A Future holding the RecordMetadata on success, or an exception on failure.
   */
  private def sendWithRetry(
      record: ProducerRecord[String, String],
      attempt: Int,
      delay: FiniteDuration
  ): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()

    // The producer's send method is asynchronous and uses a callback.
    producer.send(
      record,
      (metadata: RecordMetadata, exception: Exception) => {
        if (exception == null) {
          // Success case
          promise.success(metadata)
        } else {
          // Failure case
          promise.failure(exception)
        }
      }
    )

    promise.future.recoverWith {
      case e if isRetriable(e) && attempt < maxAttempts =>
        val nextDelay = (delay * backoffFactor).min(maxDelay)
        logger.warn(
          s"Send to Kafka topic '$topic' failed (attempt $attempt/$maxAttempts). Retrying in $nextDelay. Reason: ${e.getMessage}"
        )
        scheduleNextAttempt(sendWithRetry(record, attempt + 1, nextDelay), nextDelay)

      case NonFatal(e) =>
        logger.error(s"Send to Kafka topic '$topic' failed after $attempt attempts and will not be retried. Reason: ${e.getMessage}", e)
        Future.failed(e)
    }
  }

  /**
   * Schedules a future to be executed after a given delay.
   * @param block The block of code returning a Future to execute.
   * @param delay The duration to wait before executing.
   * @return The scheduled Future.
   */
  private def scheduleNextAttempt[T](block: => Future[T], delay: FiniteDuration): Future[T] = {
    val promise = Promise[T]()
    scheduler.schedule(
      () => promise.completeWith(Try(block).flatten),
      delay.toNanos,
      TimeUnit.NANOSECONDS
    )
    promise.future
  }

  /**
   * Determines if a given exception represents a transient, retriable error.
   * These are typically network-related issues where a subsequent attempt might succeed.
   *
   * @param e The exception to check.
   * @return true if the exception is retriable, false otherwise.
   */
  private def isRetriable(e: Throwable): Boolean = e match {
    case _: TimeoutException | _: NetworkException | _: LeaderNotAvailableException |
        _: NotEnoughReplicasException =>
      true
    case _ => false
  }

  /**
   * Gracefully shuts down the Kafka producer and the retry scheduler.
   * This should be called on application shutdown to ensure all buffered messages are sent.
   */
  override def close(): Unit = {
    logger.info("Closing KafkaSink...")
    Try(producer.close()) match {
      case Success(_) => logger.info("KafkaProducer closed successfully.")
      case Failure(e) => logger.error("Error closing KafkaProducer.", e)
    }
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch {
      case _: InterruptedException => scheduler.shutdownNow()
    }
    logger.info("KafkaSink closed.")
  }
}

object KafkaSink {
  /**
   * Factory method to create a KafkaSink instance from a Typesafe Config object.
   *
   * @param config The configuration object, expected to contain a "kafka" block.
   * @return A new instance of KafkaSink.
   */
  def fromConfig(config: Config): KafkaSink = {
    new KafkaSink(config)
  }
}