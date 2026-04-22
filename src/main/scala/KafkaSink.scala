package com.digitalasset.canton.connector

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Transactional
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.{Flow, Sink}
import com.digitalasset.canton.connector.KafkaSink.model.CantonEvent
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.util.Try

/**
 * Provides an Akka Streams Sink for publishing Canton ledger events to Kafka.
 *
 * This implementation uses the Alpakka Kafka connector and supports:
 * - Exactly-once semantics via Kafka transactions.
 * - Backpressure handling, inherent to Akka Streams.
 * - Configuration driven by Typesafe Config.
 * - Pluggable serialization (defaults to JSON).
 */
object KafkaSink {

  /**
   * Defines the data model for Canton events and their JSON serialization formats.
   * In a larger application, this would likely reside in its own `model` package.
   */
  object model {
    sealed trait CantonEvent extends Product with Serializable {
      def contractId: String
      def transactionId: String
      def eventOffset: String
    }

    final case class CreateEvent(
        contractId: String,
        templateId: String,
        payload: JsValue,
        signatories: Set[String],
        observers: Set[String],
        transactionId: String,
        eventOffset: String,
    ) extends CantonEvent

    final case class ArchiveEvent(
        contractId: String,
        templateId: String,
        transactionId: String,
        eventOffset: String,
    ) extends CantonEvent

    object CantonEventJsonProtocol extends DefaultJsonProtocol {
      // By adding a "type" field, we can discriminate between event types on deserialization.
      private def withTypeAttribute[T](format: RootJsonFormat[T], typeName: String): RootJsonFormat[T] =
        new RootJsonFormat[T] {
          def write(obj: T): JsValue = {
            val json = format.write(obj).asJsObject
            JsObject(json.fields + ("type" -> JsString(typeName)))
          }
          def read(json: JsValue): T = format.read(json)
        }

      implicit val createEventFormat: RootJsonFormat[CreateEvent] =
        withTypeAttribute(jsonFormat7(CreateEvent.apply), "create")

      implicit val archiveEventFormat: RootJsonFormat[ArchiveEvent] =
        withTypeAttribute(jsonFormat4(ArchiveEvent.apply), "archive")

      implicit val cantonEventFormat: RootJsonFormat[CantonEvent] = new RootJsonFormat[CantonEvent] {
        def write(event: CantonEvent): JsValue = event match {
          case e: CreateEvent  => e.toJson
          case e: ArchiveEvent => e.toJson
        }

        def read(json: JsValue): CantonEvent = {
          json.asJsObject.getFields("type") match {
            case Seq(JsString("create"))  => json.convertTo[CreateEvent]
            case Seq(JsString("archive")) => json.convertTo[ArchiveEvent]
            case _                        => deserializationError("CantonEvent 'type' field missing or unrecognized")
          }
        }
      }
    }
  }

  /**
   * Represents the configuration for the Kafka sink.
   *
   * @param bootstrapServers A comma-separated list of Kafka broker host:port pairs.
   * @param topic The Kafka topic to publish events to.
   * @param transactionalId A unique ID for the transactional producer. Required for exactly-once semantics.
   * @param properties Other Kafka producer properties (e.g., for security).
   */
  final case class KafkaConfig(
      bootstrapServers: String,
      topic: String,
      transactionalId: String,
      properties: Map[String, String]
  )

  object KafkaConfig {
    def from(config: Config): KafkaConfig = {
      val kafkaConfig = config.getConfig("kafka")
      val baseProperties = Map(
        "acks" -> Try(kafkaConfig.getString("acks")).getOrElse("all"),
        "linger.ms" -> Try(kafkaConfig.getDuration("linger-ms").toMillis.toString).getOrElse("100"),
        "batch.size" -> Try(kafkaConfig.getString("batch-size")).getOrElse("16384")
      )

      val customPropertiesConfig =
        if (kafkaConfig.hasPath("properties")) kafkaConfig.getConfig("properties")
        else ConfigFactory.empty()

      val customProperties = customPropertiesConfig.entrySet().asScala.map { entry =>
        entry.getKey -> entry.getValue.unwrapped().toString
      }.toMap

      KafkaConfig(
        bootstrapServers = kafkaConfig.getString("bootstrap-servers"),
        topic = kafkaConfig.getString("topic"),
        transactionalId = kafkaConfig.getString("transactional-id"),
        properties = baseProperties ++ customProperties
      )
    }
  }

  /**
   * Creates an Akka Streams Flow that sends Canton events to Kafka within a transaction.
   *
   * The flow accepts tuples of `(CantonEvent, String)`, where the second element is the
   * Canton offset. It produces messages to Kafka and, upon successful commit of the
   * Kafka transaction, emits the offset downstream. This allows for building end-to-end
   * exactly-once processing pipelines, where the Canton offset is checkpointed only
   * after the corresponding event is durably written to Kafka.
   *
   * @param config The application configuration containing Kafka settings.
   * @param system Implicit ActorSystem.
   * @return A `Flow` that processes `(CantonEvent, Offset)` tuples and emits the `Offset`.
   */
  def transactionalFlow(
      config: Config
  )(implicit system: ActorSystem): Flow[(CantonEvent, String), String, _] = {
    val kafkaConfig = KafkaConfig.from(config)
    import model.CantonEventJsonProtocol._

    val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(kafkaConfig.bootstrapServers)
      .withTransactionalId(kafkaConfig.transactionalId)
      .withProperties(kafkaConfig.properties)
      .withCloseTimeout(60.seconds) // Generous timeout for transaction cleanup

    // Alpakka's transactional flow handles begin/commit/abort logic.
    val transactionalKafkaFlow = Transactional.flow(
      producerSettings,
      kafkaConfig.transactionalId
    )

    Flow[(CantonEvent, String)]
      .map { case (event, offset) =>
        val record = new ProducerRecord[String, Array[Byte]](
          kafkaConfig.topic,
          event.contractId, // Use contractId as the key for partitioning
          event.toJson.compactPrint.getBytes("UTF-8")
        )
        // Wrap the record in a ProducerMessage, passing the offset through.
        ProducerMessage.single(record, offset)
      }
      .via(transactionalKafkaFlow)
      .map(_.passThrough) // Extract and emit the offset after successful commit.
  }

  /**
   * Creates an Akka Streams Sink that sends Canton events to Kafka.
   *
   * This is a convenience method that combines `transactionalFlow` with a sink that
   * simply discards the committed offset. Use this if you don't need to perform
   * downstream checkpointing. For true exactly-once semantics, prefer using
   * `transactionalFlow` and managing checkpoints explicitly.
   *
   * @param config The Kafka producer configuration.
   * @param system Implicit ActorSystem.
   * @return A `Sink` that consumes `(CantonEvent, String)` tuples.
   */
  def sink(config: Config)(implicit system: ActorSystem): Sink[(CantonEvent, String), Future[Done]] = {
    transactionalFlow(config).to(Sink.ignore)
  }
}