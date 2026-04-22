package com.digitalasset.canton.events.streams

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.SslContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{RestartSource, Source}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Optional
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.OptionConverters._

/**
 * CantonSourceConfig holds all the necessary configuration to connect to a Canton participant's Ledger API.
 *
 * @param host                    The hostname or IP address of the participant node.
 * @param port                    The port number of the Ledger API.
 * @param parties                 A set of party identifiers to subscribe to events for.
 * @param applicationId           The application ID used to identify this connector to the ledger.
 * @param token                   Optional JWT for authentication.
 * @param tlsEnabled              Flag to enable or disable TLS.
 * @param customCaFile            Optional path to a custom CA certificate file for TLS.
 * @param ledgerIdOverride        Optional override for the ledger ID requirement. If not set, the client discovers it.
 * @param reconnectMinBackoff     The minimum backoff duration for reconnection attempts.
 * @param reconnectMaxBackoff     The maximum backoff duration for reconnection attempts.
 * @param reconnectRandomFactor   A factor to randomize the backoff duration to avoid thundering herd issues.
 */
case class CantonSourceConfig(
    host: String,
    port: Int,
    parties: Set[String],
    applicationId: String,
    token: Option[String],
    tlsEnabled: Boolean,
    customCaFile: Option[String],
    ledgerIdOverride: Option[String],
    reconnectMinBackoff: FiniteDuration = 3.seconds,
    reconnectMaxBackoff: FiniteDuration = 30.seconds,
    reconnectRandomFactor: Double = 0.2,
)

/**
 * CantonSource provides a resilient Pekko Streams Source for consuming events from a Canton Ledger API.
 * It handles connection, authentication, and automatic reconnection with exponential backoff.
 */
object CantonSource {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a Pekko Stream Source for flat transactions (creates and archives).
   * The source will automatically reconnect on failure, using an exponential backoff strategy.
   *
   * @param config The configuration for the Canton connection.
   * @param eventFilter A filter to specify which contract templates to include.
   * @param startOffset The ledger offset to start streaming from.
   * @param endOffset An optional offset to stop streaming at. If None, the stream is infinite.
   * @param ec The execution context for asynchronous operations.
   * @param esf The execution sequencer factory, required by the Daml ledger client.
   * @return A Source that emits tuples of (Transaction, LedgerOffset) and materializes to NotUsed.
   */
  def transactions(
      config: CantonSourceConfig,
      eventFilter: EventFilter,
      startOffset: LedgerOffset,
      endOffset: Option[LedgerOffset] = None,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
  ): Source[(Transaction, LedgerOffset), NotUsed] = {

    val sourceFactory: () => Source[(Transaction, LedgerOffset), NotUsed] = () => {
      logger.info(s"Attempting to connect to Canton Ledger API at ${config.host}:${config.port}...")
      val clientFuture = buildClient(config)

      val sourceFuture: Future[Source[(Transaction, LedgerOffset), NotUsed]] =
        clientFuture
          .map { client =>
            logger.info(
              s"Successfully connected to Canton Ledger API. Starting transaction stream for parties ${config.parties
                  .mkString(", ")} from offset: $startOffset"
            )

            val transactionFilter = createTransactionFilter(config.parties, eventFilter)
            val endOffsetJava = endOffset.toJava

            client.transactionClient
              .getTransactions(startOffset, endOffsetJava, transactionFilter, verbose = true)
              .map(tx => (tx, LedgerOffset(LedgerOffset.Value.Absolute(tx.offset))))
              .wireTap(pair =>
                logger.trace(
                  s"Received transaction at offset ${pair._2.value} with ID ${pair._1.transactionId}"
                )
              )
              .mapMaterializedValue(_ => NotUsed)
          }
          .recover { case ex: Throwable =>
            logger.error(
              s"Failed to establish connection to Canton Ledger API at ${config.host}:${config.port}. Retrying after backoff...",
              ex,
            )
            Source.failed(ex)
          }
      Source.futureSource(sourceFuture).mapMaterializedValue(_ => NotUsed)
    }

    RestartSource.onFailuresWithBackoff(
      minBackoff = config.reconnectMinBackoff,
      maxBackoff = config.reconnectMaxBackoff,
      randomFactor = config.reconnectRandomFactor,
    ) { () =>
      sourceFactory()
    }
  }

  private def createTransactionFilter(
      parties: Set[String],
      eventFilter: EventFilter,
  ): TransactionFilter = {
    // If the event filter's template set is empty, it means we want all templates for the given parties.
    // The Ledger API represents this with an empty InclusiveFilters list.
    val inclusiveFilters = new InclusiveFilters(templateIds = eventFilter.templateIds.toSeq)

    val filtersByParty = parties.map { party =>
      party -> inclusiveFilters
    }.toMap

    TransactionFilter(filtersByParty = filtersByParty)
  }

  private def buildClient(
      config: CantonSourceConfig
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    val sslContext: Option[SslContext] = if (config.tlsEnabled) {
      val builder = GrpcSslContexts.forClient()
      config.customCaFile.foreach(path => builder.trustManager(new File(path)))
      Some(builder.build())
    } else {
      None
    }

    val clientConfig = LedgerClientConfiguration(
      applicationId = config.applicationId,
      ledgerIdRequirement =
        LedgerIdRequirement(config.ledgerIdOverride.getOrElse(""), enabled = config.ledgerIdOverride.isDefined),
      commandClient = CommandClientConfiguration.default,
      sslContext = sslContext.toJava,
      token = config.token.toJava,
    )

    LedgerClient.singleHost(config.host, config.port, clientConfig)
  }
}