package com.digitalasset.canton.connector

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.parser.parse
import scala.util.Try
import scala.jdk.CollectionConverters._

// NOTE: The following case classes and objects are simplified stubs of the main application's
// components (models, parser, config loader, router) to make these unit tests self-contained and executable.
// In the actual project, these would be imported from the `main` source directory.

/**
 * Represents a Canton ledger event that can be streamed.
 */
sealed trait CantonEvent {
  def templateId: String
  def contractId: String
  def offset: String
}

/**
 * Represents a contract creation event.
 */
case class CreatedEvent(
    templateId: String,
    contractId: String,
    payload: Json,
    offset: String
) extends CantonEvent

/**
 * Represents a contract archival event.
 */
case class ArchivedEvent(
    templateId: String,
    contractId: String,
    offset: String
) extends CantonEvent


/**
 * Parses raw JSON strings from the Ledger API into structured `CantonEvent` objects.
 */
object EventParser {
  def parseTransactionStream(rawJson: String): Try[Seq[CantonEvent]] = Try {
    val json = parse(rawJson).getOrElse(throw new IllegalArgumentException("Invalid JSON"))
    val offset = json.hcursor.downField("offset").as[String].getOrElse("")
    
    val eventsJson = json.hcursor.downField("events").as[List[Json]].getOrElse(List.empty)

    eventsJson.flatMap { eventJson =>
      eventJson.hcursor.downField("created").focus.map { createdJson =>
        CreatedEvent(
          templateId = createdJson.hcursor.downField("templateId").as[String].getOrElse(""),
          contractId = createdJson.hcursor.downField("contractId").as[String].getOrElse(""),
          payload = createdJson.hcursor.downField("payload").focus.getOrElse(Json.Null),
          offset = offset
        )
      } orElse {
        eventJson.hcursor.downField("archived").focus.map { archivedJson =>
          ArchivedEvent(
            templateId = archivedJson.hcursor.downField("templateId").as[String].getOrElse(""),
            contractId = archivedJson.hcursor.downField("contractId").as[String].getOrElse(""),
            offset = offset
          )
        }
      }
    }
  }
}

/**
 * Holds the routing configuration for the connector.
 */
case class AppConfig(topicMappings: Map[String, String], defaultTopic: Option[String])

/**
 * Loads and parses the application configuration into a structured `AppConfig`.
 */
object ConfigLoader {
  def load(config: Config): AppConfig = {
    val mappings = if (config.hasPath("connector.routing.topic-mappings")) {
        config.getConfig("connector.routing.topic-mappings").entrySet().asScala.map { entry =>
          entry.getKey.replace("\"", "") -> entry.getValue.unwrapped().toString
        }.toMap
    } else Map.empty[String, String]
    
    val defaultTopic = if (config.hasPath("connector.routing.default-topic")) {
      Some(config.getString("connector.routing.default-topic"))
    } else None

    AppConfig(mappings, defaultTopic)
  }
}

/**
 * Determines the destination topic/stream for a given Canton event based on configuration.
 */
class Router(config: AppConfig) {
  def getTopicFor(event: CantonEvent): Option[String] = {
    config.topicMappings.get(event.templateId).orElse(config.defaultTopic)
  }
}


/**
 * Unit tests for the core logic of the Canton event streaming connector.
 * This spec covers configuration parsing, event parsing, and routing logic
 * without requiring any external systems like a ledger or a message broker.
 */
class ConnectorSpec extends AnyWordSpec with Matchers {

  private val testConfigString =
    """
      |connector {
      |  name = "test-connector"
      |  routing {
      |    default-topic = "canton-events-default"
      |    topic-mappings {
      |      "Main:Iou" = "iou-events"
      |      "Main:Asset" = "asset-events"
      |    }
      |  }
      |}
    """.stripMargin

  private val configWithoutDefaultTopic =
    """
      |connector {
      |  routing {
      |    topic-mappings {
      |      "Main:Iou" = "iou-events"
      |    }
      |  }
      |}
    """.stripMargin

  val iouCreatedEventJson: String =
    """
    {
      "events": [
        {
          "created": {
            "eventId": "0000000000000001:0",
            "contractId": "#1:0",
            "templateId": "Main:Iou",
            "payload": {
              "issuer": "Alice::1220...",
              "owner": "Bob::1220...",
              "currency": "USD",
              "amount": "100.00"
            }
          }
        }
      ],
      "offset": "0000000000000001"
    }
    """

  val assetCreatedEventJson: String =
    """
    {
      "events": [
        {
          "created": {
            "eventId": "0000000000000002:0",
            "contractId": "#2:0",
            "templateId": "Main:Asset",
            "payload": { "isin": "US0378331005", "quantity": "50" }
          }
        }
      ],
      "offset": "0000000000000002"
    }
    """

  val iouArchivedEventJson: String =
    """
    {
      "events": [
        {
          "archived": {
            "eventId": "0000000000000003:1",
            "contractId": "#1:0",
            "templateId": "Main:Iou"
          }
        }
      ],
      "offset": "0000000000000003"
    }
    """
    
  val unknownTemplateEventJson: String =
    """
    {
      "events": [
        {
          "created": {
            "eventId": "0000000000000004:0",
            "contractId": "#4:0",
            "templateId": "Unknown:Template",
            "payload": { "data": "value" }
          }
        }
      ],
      "offset": "0000000000000004"
    }
    """
    
  val multiEventTransactionJson: String =
    """
    {
      "events": [
        {
          "archived": {
            "eventId": "0000000000000005:0",
            "contractId": "#3:0",
            "templateId": "Main:Asset"
          }
        },
        {
          "created": {
            "eventId": "0000000000000005:1",
            "contractId": "#5:0",
            "templateId": "Main:Asset",
            "payload": { "isin": "US0378331005", "quantity": "25" }
          }
        }
      ],
      "offset": "0000000000000005"
    }
    """
    
  val malformedJson: String = """{"events": [ {"created": "not_an_object"} ]}"""

  "EventParser" should {
    "correctly parse a single created event" in {
      val result = EventParser.parseTransactionStream(iouCreatedEventJson)
      result.isSuccess shouldBe true
      val events = result.get
      events should have size 1
      val created = events.head.asInstanceOf[CreatedEvent]
      created.templateId shouldEqual "Main:Iou"
      created.contractId shouldEqual "#1:0"
      created.offset shouldEqual "0000000000000001"
      created.payload.hcursor.downField("amount").as[String] shouldEqual Right("100.00")
    }

    "correctly parse a single archived event" in {
      val result = EventParser.parseTransactionStream(iouArchivedEventJson)
      result.isSuccess shouldBe true
      val events = result.get
      events should have size 1
      val archived = events.head.asInstanceOf[ArchivedEvent]
      archived.templateId shouldEqual "Main:Iou"
      archived.contractId shouldEqual "#1:0"
      archived.offset shouldEqual "0000000000000003"
    }

    "correctly parse a transaction with multiple events" in {
      val result = EventParser.parseTransactionStream(multiEventTransactionJson)
      result.isSuccess shouldBe true
      val events = result.get
      events should have size 2
      events.head shouldBe an[ArchivedEvent]
      events(1) shouldBe a[CreatedEvent]
      
      val archived = events.head.asInstanceOf[ArchivedEvent]
      archived.templateId shouldEqual "Main:Asset"
      archived.offset shouldEqual "0000000000000005"

      val created = events(1).asInstanceOf[CreatedEvent]
      created.templateId shouldEqual "Main:Asset"
      created.offset shouldEqual "0000000000000005"
    }

    "return a failure for malformed JSON" in {
      val result = EventParser.parseTransactionStream(malformedJson)
      result.isFailure shouldBe true
    }

    "return an empty list for a transaction with no events" in {
      val emptyTransaction = """{"events": [], "offset": "0000000000000006"}"""
      val result = EventParser.parseTransactionStream(emptyTransaction)
      result.isSuccess shouldBe true
      result.get should be (empty)
    }
  }


  "ConfigLoader" should {
    "load topic mappings and default topic correctly" in {
      val config = ConfigFactory.parseString(testConfigString)
      val appConfig = ConfigLoader.load(config)
      appConfig.defaultTopic should contain("canton-events-default")
      appConfig.topicMappings should contain("Main:Iou" -> "iou-events")
      appConfig.topicMappings should contain("Main:Asset" -> "asset-events")
      appConfig.topicMappings should have size 2
    }
    
    "load configuration without a default topic" in {
      val config = ConfigFactory.parseString(configWithoutDefaultTopic)
      val appConfig = ConfigLoader.load(config)
      appConfig.defaultTopic should be (None)
      appConfig.topicMappings should contain("Main:Iou" -> "iou-events")
      appConfig.topicMappings should have size 1
    }
  }


  "Router" should {
    val appConfig = ConfigLoader.load(ConfigFactory.parseString(testConfigString))
    val router = new Router(appConfig)
    
    "route a mapped create event to its specific topic" in {
      val event = EventParser.parseTransactionStream(iouCreatedEventJson).get.head
      router.getTopicFor(event) should contain("iou-events")
    }
    
    "route a mapped archive event to its specific topic" in {
      val event = EventParser.parseTransactionStream(iouArchivedEventJson).get.head
      router.getTopicFor(event) should contain("iou-events")
    }

    "route an unmapped event to the default topic" in {
      val event = EventParser.parseTransactionStream(unknownTemplateEventJson).get.head
      router.getTopicFor(event) should contain("canton-events-default")
    }
    
    "return None for an unmapped event when no default topic is configured" in {
      val noDefaultConfig = ConfigLoader.load(ConfigFactory.parseString(configWithoutDefaultTopic))
      val routerWithoutDefault = new Router(noDefaultConfig)
      val event = EventParser.parseTransactionStream(unknownTemplateEventJson).get.head
      routerWithoutDefault.getTopicFor(event) should be (None)
    }

    "route multiple events from a single transaction correctly" in {
      val events = EventParser.parseTransactionStream(multiEventTransactionJson).get
      val topics = events.flatMap(router.getTopicFor)
      topics shouldEqual Seq("asset-events", "asset-events")
    }
  }
}