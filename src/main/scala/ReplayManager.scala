package com.digitalasset.canton.connector.stream

import com.digitalasset.canton.api.v30.{GetLedgerEndRequest, LedgerOffset}
import com.digitalasset.canton.api.v30.LedgerOffset.LedgerBoundary
import com.digitalasset.canton.ledger.client.LedgerClient
import com.typesafe.scalalogging.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/**
 * Defines the strategy for where to start consuming events from the Canton ledger.
 * This configuration determines the initial offset for the event stream.
 */
sealed trait ReplayStrategy extends Product with Serializable

object ReplayStrategy {
  /** Start consuming from the very beginning of the ledger. This will process all
   * transactions from the genesis of the ledger. Use with caution on production ledgers
   * with a long history.
   */
  case object FromBeginning extends ReplayStrategy

  /** Start consuming from the current end of the ledger, processing only new transactions
   * that occur after the stream starts.
   */
  case object FromEnd extends ReplayStrategy

  /**
   * Start consuming from the last successfully committed offset for this stream.
   * This is the standard strategy for resilient, fault-tolerant streams.
   * If no offset has ever been committed, this strategy will fall back to `FromEnd`
   * to avoid an unintentional full replay.
   */
  case object FromLastKnown extends ReplayStrategy

  /** Start consuming from a specific, user-provided absolute offset. This is useful for
   * manual reprocessing of a specific range of transactions.
   */
  final case class FromSpecificOffset(offset: LedgerOffset) extends ReplayStrategy
}

/**
 * An abstraction for persisting and retrieving ledger offsets for a given stream.
 * This enables fault tolerance and allows the connector to resume from where it left off
 * after a restart or failure.
 */
trait OffsetStore {
  /**
   * Persists the given offset for a specific stream ID.
   *
   * @param streamId A unique identifier for the data stream (e.g., a consumer group ID or sink name).
   * @param offset   The ledger offset to store.
   * @return A Future that completes when the operation is done.
   */
  def store(streamId: String, offset: LedgerOffset): Future[Unit]

  /**
   * Retrieves the last persisted offset for a specific stream ID.
   *
   * @param streamId A unique identifier for the data stream.
   * @return A Future containing an Option of the last known offset, or None if no offset has been stored.
   */
  def retrieve(streamId: String): Future[Option[LedgerOffset]]
}

/**
 * A simple, non-persistent, in-memory implementation of OffsetStore.
 * This implementation is suitable for testing, development, or scenarios where
 * stream resilience across application restarts is not a requirement.
 */
class InMemoryOffsetStore extends OffsetStore with LazyLogging {
  private val storeMap = new TrieMap[String, LedgerOffset]()

  override def store(streamId: String, offset: LedgerOffset): Future[Unit] = {
    logger.debug(s"In-memory store for stream '$streamId' updating offset to ${offset.toProtoString}")
    storeMap.put(streamId, offset)
    Future.successful(())
  }

  override def retrieve(streamId: String): Future[Option[LedgerOffset]] = {
    val offset = storeMap.get(streamId)
    logger.debug(s"In-memory store for stream '$streamId' retrieving offset: ${offset.map(_.toProtoString)}")
    Future.successful(offset)
  }
}

/**
 * Manages the starting offset for a ledger event stream based on a defined replay strategy
 * and handles the committing of new offsets as events are processed.
 *
 * This class is central to the connector's ability to provide at-least-once or exactly-once
 * processing guarantees by ensuring that processing can be safely resumed after a restart.
 *
 * @param streamId     A unique identifier for this particular data stream. This is used as the key
 *                     in the OffsetStore.
 * @param strategy     The ReplayStrategy to determine the stream's starting point.
 * @param offsetStore  The persistent store for tracking the last processed offset.
 * @param ledgerClient A client connected to the Canton ledger, used to query for ledger boundaries if needed.
 * @param ec           An execution context for running asynchronous operations.
 */
class ReplayManager(
    streamId: String,
    strategy: ReplayStrategy,
    offsetStore: OffsetStore,
    ledgerClient: LedgerClient
)(implicit ec: ExecutionContext)
    extends LazyLogging {

  /**
   * Determines the initial offset from which to start streaming from the ledger,
   * based on the configured ReplayStrategy.
   *
   * @return A Future containing the calculated starting LedgerOffset.
   */
  def getStartOffset(): Future[LedgerOffset] = {
    strategy match {
      case ReplayStrategy.FromBeginning =>
        logger.info(s"Stream '$streamId': Starting replay from the beginning of the ledger.")
        Future.successful(
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.LEDGER_BEGIN))
        )

      case ReplayStrategy.FromEnd =>
        logger.info(s"Stream '$streamId': Starting from the current end of the ledger.")
        fetchLedgerEnd()

      case ReplayStrategy.FromSpecificOffset(offset) =>
        logger.info(s"Stream '$streamId': Starting from specific offset: ${offset.toProtoString}")
        Future.successful(offset)

      case ReplayStrategy.FromLastKnown =>
        logger.info(s"Stream '$streamId': Attempting to resume from the last known offset.")
        offsetStore.retrieve(streamId).flatMap {
          case Some(offset) =>
            logger.info(s"Stream '$streamId': Resuming from last known offset: ${offset.toProtoString}")
            Future.successful(offset)
          case None =>
            logger.warn(
              s"Stream '$streamId': No last known offset found for replay strategy 'FromLastKnown'. " +
                "Defaulting to start from the current ledger end to prevent full replay."
            )
            fetchLedgerEnd()
        }
    }
  }

  /**
   * Commits an offset to the OffsetStore, marking it as successfully processed.
   * This should be called after the corresponding events have been successfully
   * written to the downstream sink.
   *
   * @param offset The LedgerOffset to commit.
   * @return A Future that completes when the offset has been stored.
   */
  def commitOffset(offset: LedgerOffset): Future[Unit] = {
    logger.trace(s"Stream '$streamId': Committing offset ${offset.toProtoString}")
    offsetStore.store(streamId, offset)
  }

  private def fetchLedgerEnd(): Future[LedgerOffset] = {
    ledgerClient.transactionService
      .getLedgerEnd(GetLedgerEndRequest())
      .map(response =>
        response.offset.getOrElse {
          // This case is highly unlikely with a healthy, running ledger.
          logger.error(
            "GetLedgerEnd response did not contain an offset. " +
              "This may indicate a problem with the ledger API server. Defaulting to ledger begin."
          )
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.LEDGER_BEGIN))
        }
      )
  }
}