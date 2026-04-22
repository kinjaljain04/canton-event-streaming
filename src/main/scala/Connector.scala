package com.digitalasset.canton.events

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.digitalasset.canton.events.model.{LedgerEvent, LedgerOffset}
import com.digitalasset.canton.events.config.ConnectorConfig
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * Represents a running instance of the Canton Event Streaming Connector.
 *
 * @param shutdown Handle to gracefully shut down the connector stream.
 * @param done A Future that completes when the stream terminates.
 */
case class RunningConnector(shutdown: () => Future[Done], done: Future[Done])

/**
 * The main connector class that wires together a Canton Ledger API source
 * with a configurable sink (e.g., Kafka, Pulsar, Kinesis).
 *
 * It is responsible for building and running a resilient Akka Stream that:
 * - Fetches events from a specific offset.
 * - Filters events based on user-defined criteria.
 * - Handles backpressure automatically.
 * - Processes events concurrently.
 * - Sends events that fail to be processed to a Dead Letter Queue (DLQ).
 * - Commits offsets after successful processing to provide at-least-once delivery guarantees.
 * - Restarts automatically on transient failures with an exponential backoff strategy.
 *
 * @param config The connector's configuration.
 * @param sourceProvider A provider for the Canton Ledger API event source.
 * @param sink A generic Akka Sink for publishing events. It takes a tuple of (LedgerEvent, LedgerOffset).
 * @param offsetStore A durable store for reading and writing the last processed ledger offset.
 * @param filter The filter to apply to incoming ledger events.
 * @param dlq The dead letter queue for failed events.
 * @param metrics A metrics collector for monitoring.
 * @param system The Akka ActorSystem.
 * @param ec The execution context for futures.
 */
class Connector(
    config: ConnectorConfig,
    sourceProvider: CantonSourceProvider,
    sink: Sink[(LedgerEvent, LedgerOffset), Future[Done]],
    offsetStore: OffsetStore,
    filter: EventFilter,
    dlq: DeadLetterQueue,
    metrics: ConnectorMetrics
)(implicit system: ActorSystem, ec: ExecutionContext) extends LazyLogging {

  /**
   * Starts the event streaming process.
   *
   * The method builds and materializes the Akka Stream. It returns a `RunningConnector`
   * instance which provides a handle to shut down the stream and a Future that
   * completes upon termination.
   *
   * @return A RunningConnector instance.
   */
  def run(): RunningConnector = {
    logger.info(s"Starting Canton Event Streaming Connector: ${config.name}")
    logger.info(s"Restart settings: minBackoff=${config.restart.minBackoff}, maxBackoff=${config.restart.maxBackoff}")

    val killSwitch = KillSwitches.shared("canton-connector-stream")

    val streamSource = RestartSource.onFailuresWithBackoff(
      minBackoff = config.restart.minBackoff,
      maxBackoff = config.restart.maxBackoff,
      randomFactor = 0.2,
      maxRestarts = config.restart.maxRestarts.getOrElse(-1) // -1 for infinite restarts
    ) { () =>
      buildStreamFlow(killSwitch)
    }

    val doneFuture = streamSource
      .toMat(Sink.ignore)(Keep.right)
      .run()

    doneFuture.onComplete {
      case scala.util.Success(_) =>
        logger.info(s"Connector ${config.name} stream completed successfully.")
      case scala.util.Failure(e) =>
        logger.error(s"Connector ${config.name} stream failed and will not restart.", e)
    }

    val shutdown = () => {
      logger.info(s"Shutdown requested for connector ${config.name}. Draining stream...")
      killSwitch.shutdown()
      // The stream might take a while to drain. The `done` future will complete when it's finished.
      // We don't wait for it here, allowing for a fast shutdown signal.
      Future.successful(Done)
    }

    RunningConnector(shutdown, doneFuture)
  }

  private def buildStreamFlow(killSwitch: SharedKillSwitch): Source[Done, _] = {
    Source.futureSource {
      // 1. Asynchronously load the last processed offset before starting the stream.
      offsetStore.loadOffset().map { startOffset =>
        logger.info(s"Starting event stream for connector '${config.name}' from offset: $startOffset")

        // 2. Create the Canton event source starting from that offset.
        sourceProvider.source(startOffset)
          .via(killSwitch.flow) // Attach the kill switch to allow graceful shutdown.
          .map(metrics.eventReceived) // Record that an event has been received from the ledger.

          // 3. Apply user-defined filters to process only relevant events.
          .filter(filter.shouldProcess)

          // 4. Group events into batches for more efficient processing and offset committing.
          .groupedWithin(config.processing.batchSize, config.processing.batchTimeout)
          .filter(_.nonEmpty)

          // 5. Process batches concurrently while preserving the order of batches.
          // This is the main processing stage.
          .mapAsync(config.processing.parallelism) { batch =>
            processBatch(batch)
          }
      }
    }
  }

  private def processBatch(batch: Seq[LedgerEvent]): Future[Done] = {
    val eventsWithOffsets = batch.map(event => (event, event.offset))
    val lastEventInBatch = eventsWithOffsets.last

    Source(eventsWithOffsets)
      .runWith(sink) // Send the entire batch to the configured sink (e.g., Kafka producer sink).
      .flatMap { _ =>
        // If the sink operation succeeds, commit the offset of the last event in the batch.
        offsetStore.saveOffset(lastEventInBatch._2)
      }
      .map { _ =>
        metrics.batchProcessed(batch.size.toLong)
        logger.debug(s"Successfully processed and committed batch up to offset ${lastEventInBatch._2}")
        Done
      }
      .recoverWith {
        case NonFatal(e) =>
          // If sinking or offset commit fails, send the entire batch to the DLQ.
          logger.error(s"Failed to process batch ending at offset ${lastEventInBatch._2}. Sending to DLQ.", e)
          metrics.batchFailed(batch.size.toLong)
          dlq.sendBatch(batch, e)
      }
  }
}

/**
 * An abstraction for providing a source of Canton ledger events.
 * This allows for easy testing with mock sources.
 */
trait CantonSourceProvider {
  /**
   * Creates an Akka Stream Source that emits LedgerEvents starting from the given offset.
   *
   * @param offset The ledger offset to start streaming from.
   * @return An Akka Stream Source of LedgerEvent.
   */
  def source(offset: LedgerOffset): Source[LedgerEvent, _]
}

/**
 * An abstraction for a durable key-value store used to persist the last
 * successfully processed ledger offset. This is critical for achieving
 * at-least-once delivery semantics and resuming correctly after a restart.
 */
trait OffsetStore {
  /**
   * Asynchronously loads the last saved offset.
   * If no offset is found, it should return a default starting offset (e.g., LedgerBegin).
   *
   * @return A Future containing the LedgerOffset.
   */
  def loadOffset(): Future[LedgerOffset]

  /**
   * Asynchronously saves the given offset.
   * This should be called after a batch of events has been successfully processed and sent to the sink.
   *
   * @param offset The LedgerOffset to save.
   * @return A Future that completes when the save operation is done.
   */
  def saveOffset(offset: LedgerOffset): Future[Done]
}