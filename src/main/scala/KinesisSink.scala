package com.digitalasset.canton.connector

import akka.Done
import akka.stream.alpakka.kinesis.scaladsl.KinesisFlow
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.digitalasset.canton.connector.model.LedgerEvent
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import spray.json._

import java.net.URI
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.{Try, Success, Failure}

// In a real project, these models and JSON formats would reside in their own files
// within a `model` package, e.g., `com.digitalasset.canton.connector.model`.
package model {
  sealed trait LedgerEvent {
    def eventId: String // A unique identifier for the event, e.g., transaction ID
    def offset: String // The ledger offset for checkpointing
  }

  case class TransactionEvent(
      eventId: String,
      workflowId: String,
      offset: String,
      contractEvents: Seq[ContractEvent]
  ) extends LedgerEvent

  sealed trait ContractEvent
  case class CreatedEvent(templateId: String, contractId: String, payload: JsValue)
      extends ContractEvent
  case class ArchivedEvent(templateId: String, contractId: String) extends ContractEvent

  object JsonProtocol extends DefaultJsonProtocol {
    implicit val createdEventFormat: RootJsonFormat[CreatedEvent] = jsonFormat3(CreatedEvent)
    implicit val archivedEventFormat: RootJsonFormat[ArchivedEvent] = jsonFormat2(ArchivedEvent)

    implicit object ContractEventFormat extends RootJsonFormat[ContractEvent] {
      def write(event: ContractEvent): JsValue = event match {
        case e: CreatedEvent =>
          JsObject("type" -> JsString("created"), "event" -> e.toJson)
        case e: ArchivedEvent =>
          JsObject("type" -> JsString("archived"), "event" -> e.toJson)
      }
      def read(json: JsValue): ContractEvent =
        json.asJsObject.getFields("type", "event") match {
          case Seq(JsString("created"), eventJson) => eventJson.convertTo[CreatedEvent]
          case Seq(JsString("archived"), eventJson) => eventJson.convertTo[ArchivedEvent]
          case _ => deserializationError("ContractEvent expected")
        }
    }

    implicit val transactionEventFormat: RootJsonFormat[TransactionEvent] = jsonFormat4(
      TransactionEvent
    )

    implicit object LedgerEventFormat extends RootJsonFormat[LedgerEvent] {
      def write(event: LedgerEvent): JsValue = event match {
        case e: TransactionEvent =>
          JsObject("type" -> JsString("transaction"), "event" -> e.toJson)
      }
      def read(json: JsValue): LedgerEvent =
        json.asJsObject.getFields("type", "event") match {
          case Seq(JsString("transaction"), eventJson) => eventJson.convertTo[TransactionEvent]
          case _ => deserializationError("LedgerEvent expected")
        }
    }
  }
}

/**
 * Provides a Sink that sends LedgerEvent objects to AWS Kinesis Data Streams.
 *
 * This implementation uses the Alpakka Kinesis connector, which handles batching,
 * backpressure, and retries according to the configured settings. It serializes
 * events to JSON and uses a configurable field for the partition key to enable
 * flexible data sharding.
 */
object KinesisSink {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Represents the configuration for the Kinesis sink.
   *
   * @param streamName          The name of the Kinesis stream.
   * @param region              The AWS region of the stream.
   * @param endpointOverride    Optional endpoint override for local testing (e.g., with LocalStack).
   * @param partitionKeyField   The field in the LedgerEvent JSON to use as the partition key.
   *                            This determines which shard the record goes to. Defaults to 'eventId'.
   * @param parallelism         The number of concurrent PutRecords requests to Kinesis.
   * @param maxBatchSize        The maximum number of records to batch into a single request (Kinesis API limit is 500).
   * @param maxRecordsPerSecond The maximum number of records to send per second.
   * @param maxBytesPerSecond   The maximum number of bytes to send per second.
   * @param retryInitialBackoff The initial backoff duration for retries on failed requests.
   * @param maxRetries          The maximum number of times to retry a failed request.
   */
  final case class KinesisConfig(
      streamName: String,
      region: String,
      endpointOverride: Option[String],
      partitionKeyField: String,
      parallelism: Int,
      maxBatchSize: Int,
      maxRecordsPerSecond: Int,
      maxBytesPerSecond: Int,
      retryInitialBackoff: FiniteDuration,
      maxRetries: Int
  )

  object KinesisConfig {
    def fromRootConfig(config: Config): KinesisConfig = {
      val kinesisConfig = config.getConfig("connector.sink.kinesis")
      KinesisConfig(
        streamName = kinesisConfig.getString("stream-name"),
        region = kinesisConfig.getString("region"),
        endpointOverride = Try(kinesisConfig.getString("endpoint-override")).toOption.filter(_.nonEmpty),
        partitionKeyField = kinesisConfig.getString("partition-key-field"),
        parallelism = kinesisConfig.getInt("parallelism"),
        maxBatchSize = kinesisConfig.getInt("max-batch-size"),
        maxRecordsPerSecond = kinesisConfig.getInt("max-records-per-second"),
        maxBytesPerSecond = kinesisConfig.getInt("max-bytes-per-second"),
        retryInitialBackoff =
          kinesisConfig.getDuration("retry-initial-backoff").toNanos.nanos,
        maxRetries = kinesisConfig.getInt("max-retries")
      )
    }
  }

  /**
   * Creates an AWS Kinesis Sink.
   *
   * @param config The root application configuration.
   * @param ec     An implicit ExecutionContext for async operations.
   * @return A Sink that accepts LedgerEvents and materializes to a Future[Done].
   */
  def apply(config: Config)(implicit ec: ExecutionContext): Sink[LedgerEvent, Future[Done]] = {
    val kinesisConfig = KinesisConfig.fromRootConfig(config)
    logger.info(s"Initializing Kinesis Sink for stream '${kinesisConfig.streamName}' in region '${kinesisConfig.region}'")

    implicit val kinesisClient: KinesisAsyncClient = createKinesisClient(kinesisConfig)

    val flowSettings = KinesisFlowSettings()
      .withParallelism(kinesisConfig.parallelism)
      .withMaxBatchSize(kinesisConfig.maxBatchSize)
      .withMaxRecordsPerSecond(kinesisConfig.maxRecordsPerSecond)
      .withMaxBytesPerSecond(kinesisConfig.maxBytesPerSecond)
      .withRetryInitialTimeout(kinesisConfig.retryInitialBackoff.toJava)
      .withMaxRetries(kinesisConfig.maxRetries)

    val kinesisFlow: Flow[LedgerEvent, _, _] = Flow[LedgerEvent]
      .map(eventToRequestEntry(kinesisConfig.partitionKeyField))
      .via(KinesisFlow(kinesisConfig.streamName, flowSettings))
      .map { result =>
        if (result.failedRecords.nonEmpty) {
          logger.warn(
            s"Failed to publish ${result.failedRecords.size} of ${result.records.size} records to Kinesis stream '${kinesisConfig.streamName}'. First error: ${result.failedRecords.head.errorMessage()}"
          )
        } else {
          logger.trace(s"Successfully published batch of ${result.records.size} records to Kinesis.")
        }
        result
      }

    kinesisFlow.toMat(Sink.ignore)((_, future) => future)
  }

  private def createKinesisClient(config: KinesisConfig): KinesisAsyncClient = {
    val clientBuilder = KinesisAsyncClient
      .builder()
      .region(Region.of(config.region))
      .credentialsProvider(DefaultCredentialsProvider.create())

    config.endpointOverride.foreach { endpoint =>
      logger.warn(s"Overriding Kinesis endpoint to: $endpoint. THIS SHOULD ONLY BE USED FOR LOCAL TESTING.")
      clientBuilder.endpointOverride(URI.create(endpoint))
    }

    clientBuilder.build()
  }

  /**
   * Converts a LedgerEvent into a PutRecordsRequestEntry for the Kinesis API.
   * It serializes the event to JSON and extracts a partition key from the resulting JSON.
   */
  private def eventToRequestEntry(
      partitionKeyField: String
  )(event: LedgerEvent): PutRecordsRequestEntry = {
    import model.JsonProtocol._

    val eventJson = event.toJson
    val eventJsonString = eventJson.compactPrint
    val eventBytes = ByteString(eventJsonString)

    // Extract partition key from the specified field in the top-level event JSON object.
    // This is crucial for distributing records across shards.
    // If the field is not found or is not a string, we fall back to the eventId.
    val partitionKey = eventJson.asJsObject.getFields("event").headOption
      .flatMap(_.asJsObject.fields.get(partitionKeyField)) match {
        case Some(JsString(value)) => value
        case _ =>
          logger.trace(s"Partition key field '$partitionKeyField' not found in event payload. Falling back to eventId.")
          event.eventId
      }

    PutRecordsRequestEntry
      .builder()
      .data(eventBytes.asSdkBytes)
      .partitionKey(partitionKey)
      .build()
  }
}