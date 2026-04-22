package com.digitalasset.canton.events

import com.typesafe.config.Config
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import spray.json._

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Manages Avro schema registration and serialization for Canton ledger events.
 *
 * This class handles the interaction with a Confluent Schema Registry instance,
 * defining, registering, and using Avro schemas to serialize Canton events.
 *
 * @param client The underlying Schema Registry client.
 * @param topic The target Kafka topic, used for resolving the schema subject name
 *              (assuming TopicNameStrategy).
 */
class SchemaRegistry(client: SchemaRegistryClient, topic: String) {
  
  // Eagerly register all known event schemas on initialization.
  // This ensures that schemas are available before the first message is produced.
  registerSchemas()

  /**
   * Registers the Avro schemas for all Canton event types with the Schema Registry.
   *
   * It registers a union schema containing both CreatedEvent and ArchivedEvent,
   * allowing a single topic to carry multiple event types. This requires consumers
   * to be able to handle the union type.
   *
   * The subject name is determined by the `TopicNameStrategy` (e.g., "my-topic-value").
   */
  def registerSchemas(): Unit = {
    val valueSubject = s"$topic-value"
    val unionSchema = Schema.createUnion(
      SchemaRegistry.CreateEventSchema,
      SchemaRegistry.ArchivedEventSchema
    )
    client.register(valueSubject, unionSchema)
    ()
  }

  /**
   * Converts a Canton event into an Avro GenericRecord according to its schema.
   * This generic record can then be serialized by the Avro serializer.
   *
   * @param event The CantonEvent (Created or Archived) to convert.
   * @return A GenericRecord representing the event.
   */
  def toGenericRecord(event: CantonEvent): GenericRecord = {
    event match {
      case create: CantonEvent.Created =>
        val record = new GenericData.Record(SchemaRegistry.CreateEventSchema)
        record.put("eventId", create.eventId)
        record.put("contractId", create.contractId)
        record.put("templateId", create.templateId)
        // Serialize the payload JsValue to a JSON string for maximum flexibility.
        record.put("payload", create.payload.compactPrint)
        record.put("signatories", create.signatories.asJava)
        record.put("observers", create.observers.asJava)
        record.put("transactionId", create.transactionId)
        record.put("ledgerEffectiveTime", create.ledgerEffectiveTime)
        record

      case archive: CantonEvent.Archived =>
        val record = new GenericData.Record(SchemaRegistry.ArchivedEventSchema)
        record.put("eventId", archive.eventId)
        record.put("contractId", archive.contractId)
        record.put("templateId", archive.templateId)
        record.put("signatories", archive.signatories.asJava)
        record.put("observers", archive.observers.asJava)
        record.put("transactionId", archive.transactionId)
        record
    }
  }

  /**
   * Provides a configured KafkaAvroSerializer for use with a Kafka producer.
   * The serializer will use this SchemaRegistry instance's client to interact
   * with the schema registry. Auto-registration is disabled as we handle it explicitly.
   *
   * @return A configured KafkaAvroSerializer instance for message values.
   */
  def avroValueSerializer(): KafkaAvroSerializer = {
    val serializer = new KafkaAvroSerializer(client)
    val configMap = Map(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> client.getBaseUrl,
      // We manage registration explicitly, so disable auto-registration by the serializer.
      AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS -> "false"
    ).asJava
    serializer.configure(configMap, false) // `isKey` is false for a value serializer
    serializer
  }
}

object SchemaRegistry {

  /**
   * Factory method to create a SchemaRegistry instance from application configuration.
   *
   * @param config The application configuration (e.g., from Typesafe Config).
   * @param topic  The Kafka topic this registry will be used for.
   * @return A Try-wrapped SchemaRegistry instance, capturing any initialization errors.
   */
  def fromConfig(config: Config, topic: String): Try[SchemaRegistry] = Try {
    val schemaRegistryUrl = config.getString("connector.sink.kafka.schema-registry-url")
    // Use a cached client for performance.
    val client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    new SchemaRegistry(client, topic)
  }

  // Define the Avro schema for a Canton Create Event.
  // The contract payload is stored as a JSON string for flexibility, avoiding the need
  // to generate and manage a unique Avro schema for every Daml template.
  private val createEventSchemaString =
    """
      |{
      |  "type": "record",
      |  "name": "CreatedEvent",
      |  "namespace": "com.digitalasset.canton.events.avro",
      |  "fields": [
      |    {"name": "eventId", "type": "string", "doc": "Unique identifier for the event"},
      |    {"name": "contractId", "type": "string", "doc": "The ID of the created contract"},
      |    {"name": "templateId", "type": "string", "doc": "The template ID of the created contract"},
      |    {"name": "payload", "type": "string", "doc": "The contract payload, serialized as a JSON string"},
      |    {"name": "signatories", "type": {"type": "array", "items": "string"}},
      |    {"name": "observers", "type": {"type": "array", "items": "string"}},
      |    {"name": "transactionId", "type": "string", "doc": "The ID of the transaction that created the contract"},
      |    {"name": "ledgerEffectiveTime", "type": "string", "doc": "ISO-8601 timestamp of when the contract became active"}
      |  ]
      |}
      |""".stripMargin

  // Define the Avro schema for a Canton Archive Event.
  private val archivedEventSchemaString =
    """
      |{
      |  "type": "record",
      |  "name": "ArchivedEvent",
      |  "namespace": "com.digitalasset.canton.events.avro",
      |  "fields": [
      |    {"name": "eventId", "type": "string", "doc": "Unique identifier for the event"},
      |    {"name": "contractId", "type": "string", "doc": "The ID of the archived contract"},
      |    {"name": "templateId", "type": "string", "doc": "The template ID of the archived contract"},
      |    {"name": "signatories", "type": {"type": "array", "items": "string"}, "doc": "Stakeholders of the archived contract"},
      |    {"name": "observers", "type": {"type": "array", "items": "string"}, "doc": "Stakeholders of the archived contract"},
      |    {"name": "transactionId", "type": "string", "doc": "The ID of the transaction that archived the contract"}
      |  ]
      |}
      |""".stripMargin

  private val parser = new Schema.Parser()

  // Parsed Avro Schema objects, ready for use.
  val CreateEventSchema: Schema = parser.parse(createEventSchemaString)
  val ArchivedEventSchema: Schema = parser.parse(archivedEventSchemaString)
}