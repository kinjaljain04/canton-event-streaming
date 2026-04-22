package com.digitalasset.canton.events.streams

import akka.Done
import akka.actor.ActorSystem
import com.digitalasset.canton.events.streams.KafkaSink.KafkaSinkConfig
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside}

import scala.concurrent.Future
import scala.concurrent.duration._

// Dummy event definitions for the purpose of the test.
// In a real project, these would be imported from a shared domain module.
sealed trait LedgerEvent extends Product with Serializable {
  def offset: String
  def transactionId: String
}

object LedgerEvent {
  // A simple JSON representation for testing purposes.
  // A real implementation would likely use a robust JSON library like circe or play-json.
  def toJson(event: LedgerEvent): String = event match {
    case Created(offset, txId, cid, tid, payload) =>
      s"""{"type":"created","offset":"$offset","transactionId":"$txId","contractId":"$cid","templateId":"$tid","payload":$payload}"""
    case Archived(offset, txId, cid, tid) =>
      s"""{"type":"archived","offset":"$offset","transactionId":"$txId","contractId":"$cid","templateId":"$tid"}"""
  }

  case class Created(offset: String, transactionId: String, contractId: String, templateId: String, payload: String) extends LedgerEvent
  case class Archived(offset: String, transactionId: String, contractId: String, templateId: String) extends LedgerEvent
}

class KafkaSinkSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll with AsyncMockFactory with Inside with Eventually with EmbeddedKafka {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(10, Seconds)),
    interval = scaled(Span(200, Millis))
  )

  private implicit val system: ActorSystem = ActorSystem("KafkaSinkSpec")
  private implicit val stringSerializer: Serializer[String] = new StringSerializer()
  private implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()

  private val testTopic = "ledger-events-test"

  // Use a custom port to avoid clashes in CI environments
  private implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 9092,
    zooKeeperPort = 2181,
    schemaRegistryPort = 8081
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
    // Explicitly create the topic before running tests to ensure it exists
    createCustomTopic(testTopic)
  }

  override def afterAll(): Unit = {
    system.terminate()
    EmbeddedKafka.stop()
    super.afterAll()
  }

  private def testKafkaConfig(transactionalId: String): KafkaSinkConfig = {
    val configStr =
      s"""
         |bootstrap-servers = "localhost:${kafkaConfig.kafkaPort}"
         |topic = "$testTopic"
         |transactional-id = "$transactionalId"
         |properties = {
         |  "acks" = "all"
         |  // Required for transactional/exactly-once semantics
         |  "enable.idempotence" = "true"
         |  // Limit to one in-flight request to preserve ordering guarantees with retries
         |  "max.in.flight.requests.per.connection" = "1"
         |  "retries" = "5"
         |}
         |""".stripMargin
    KafkaSinkConfig(ConfigFactory.parseString(configStr))
  }

  "KafkaSink" should {

    "send a single created event to a topic" in {
      val sinkConfig = testKafkaConfig("tx-id-single")
      val mockReplayManager = mock[ReplayManager]
      val sink = new KafkaSink(sinkConfig, mockReplayManager)

      val event = LedgerEvent.Created("offset-1", "tx-1", "cid-1", "MyModule:MyTemplate", """{"field":"value"}""")
      val events = List(event)

      (mockReplayManager.commitOffset _).expects(event.offset).returning(Future.successful(Done)).once()

      for {
        _ <- sink.send(events)
        consumedMessage <- Future {
          consumeFirstStringMessageFrom(testTopic)
        }
      } yield {
        consumedMessage shouldBe LedgerEvent.toJson(event)
        succeed
      }
    }

    "send a batch of mixed events transactionally" in {
      val sinkConfig = testKafkaConfig("tx-id-batch")
      val mockReplayManager = mock[ReplayManager]
      val sink = new KafkaSink(sinkConfig, mockReplayManager)

      val events = List(
        LedgerEvent.Created("offset-10", "tx-10", "cid-10", "MyModule:TemplateA", "{}"),
        LedgerEvent.Archived("offset-11", "tx-11", "cid-10", "MyModule:TemplateA"),
        LedgerEvent.Created("offset-12", "tx-12", "cid-11", "MyModule:TemplateB", """{"value": 100}""")
      )
      val lastOffset = events.last.offset

      (mockReplayManager.commitOffset _).expects(lastOffset).returning(Future.successful(Done)).once()

      for {
        _ <- sink.send(events)
        consumedMessages <- Future {
          consumeNumberStringMessagesFrom(testTopic, 3, timeout = 5.seconds)
        }
      } yield {
        consumedMessages should have size 3
        consumedMessages should contain theSameElementsInOrderAs events.map(LedgerEvent.toJson)
      }
    }

    "not expose messages to consumers if the transaction is aborted" in {
      val sinkConfig = testKafkaConfig("tx-id-abort")
      // Simulate an error during offset commit to trigger a transaction abort
      val mockReplayManager = mock[ReplayManager]
      val sink = new KafkaSink(sinkConfig, mockReplayManager)

      val events = List(
        LedgerEvent.Created("offset-20", "tx-20", "cid-20", "MyModule:TemplateC", "{}")
      )
      val commitException = new RuntimeException("DB connection failed!")

      (mockReplayManager.commitOffset _)
        .expects(events.last.offset)
        .returning(Future.failed(commitException))
        .once()

      val result = sink.send(events)

      // The send operation should fail, propagating the exception from the offset manager
      recoverToSucceededIf[RuntimeException] {
        result
      }

      // No messages should be visible to a read_committed consumer
      val consumerConfig = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
      val consumed = consumeNumberMessagesFromTopics(Set(testTopic), 0, timeout = 3.seconds, customConsumerConfig = consumerConfig)
      consumed.get(testTopic) shouldBe Some(List.empty)
    }

    "correctly handle sending an empty batch of events" in {
      val sinkConfig = testKafkaConfig("tx-id-empty")
      val mockReplayManager = mock[ReplayManager]
      val sink = new KafkaSink(sinkConfig, mockReplayManager)

      // The offset manager should not be called for an empty batch
      (mockReplayManager.commitOffset _).expects(*).never()

      for {
        result <- sink.send(List.empty)
      } yield {
        result shouldBe Done
      }
    }

    "ensure exactly-once delivery by retrying a failed batch" in {
      val sinkConfig = testKafkaConfig("tx-id-retry")
      val mockReplayManager = mock[ReplayManager]
      val sink = new KafkaSink(sinkConfig, mockReplayManager)

      val events = List(
        LedgerEvent.Created("offset-30", "tx-30", "cid-30", "MyModule:TemplateD", "{}")
      )
      val lastOffset = events.last.offset
      val commitException = new RuntimeException("Transient commit failure")

      // === First attempt: Fails during offset commit ===
      (mockReplayManager.commitOffset _)
        .expects(lastOffset)
        .returning(Future.failed(commitException))
        .once()

      // The sink should abort the Kafka transaction and propagate the failure.
      val firstAttempt = sink.send(events)
      recoverToSucceededIf[RuntimeException] {
        firstAttempt
      }

      // === Second attempt: Simulates connector replaying the batch ===
      (mockReplayManager.commitOffset _)
        .expects(lastOffset)
        .returning(Future.successful(Done))
        .once()

      for {
        _ <- sink.send(events)
        // We expect exactly one message to be committed to the topic eventually.
        consumedMessages <- Future {
          consumeNumberStringMessagesFrom(testTopic, 1, timeout = 5.seconds)
        }
      } yield {
        // Due to producer idempotency, the message is written only once across both attempts.
        consumedMessages should have size 1
        consumedMessages.head shouldBe LedgerEvent.toJson(events.head)
      }
    }
  }
}