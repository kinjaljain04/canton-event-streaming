/*
 * Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.digitalasset.canton.events.streams.dlq

import java.io.{FileWriter, PrintWriter, StringWriter}
import java.nio.file.{Files, Path}
import java.time.Instant
import scala.util.control.NonFatal

/**
 * A generic interface for a Dead-Letter Queue (DLQ).
 *
 * A DLQ is a destination for events that could not be processed successfully after a
 * configured number of retries. Storing these events allows for later inspection,
 * debugging, and potential manual reprocessing, preventing data loss without halting
 * the main event processing pipeline.
 *
 * @tparam E The type of the event to be published.
 */
trait DeadLetterQueue[E] extends AutoCloseable {

  /**
   * Publishes a failed event to the DLQ.
   *
   * This method should be implemented to be thread-safe, as it may be called
   * concurrently from multiple processing threads. Implementations should also be
   * resilient to their own failures (e.g., if a file system is full, it should log
   * the error rather than propagating an exception that could crash the service).
   *
   * @param event The original event that failed processing.
   * @param cause The throwable that represents the reason for the failure.
   * @param metadata A map of key-value pairs providing additional context about the
   *                 event, such as its original ledger offset, partition key, etc.
   */
  def publish(event: E, cause: Throwable, metadata: Map[String, String]): Unit

  /**
   * Closes any underlying resources, such as file handles or network connections.
   */
  override def close(): Unit = () // Default no-op implementation
}

/**
 * A no-op implementation of a Dead-Letter Queue that discards all events.
 * This can be used in configurations where failed events should be ignored.
 *
 * @tparam E The type of the event.
 */
class NoOpDeadLetterQueue[E] extends DeadLetterQueue[E] {
  override def publish(event: E, cause: Throwable, metadata: Map[String, String]): Unit = ()
}

object NoOpDeadLetterQueue {
  def apply[E](): NoOpDeadLetterQueue[E] = new NoOpDeadLetterQueue[E]()
}

/**
 * A file-based implementation of a Dead-Letter Queue.
 *
 * It writes failed events as JSON objects, one per line, to a specified file.
 * This implementation is thread-safe. If it fails to write to the file, it logs the
 * error to stderr and continues, ensuring the main application is not affected.
 *
 * The output format is a JSON line (`.jsonl`) file, with each line being a valid JSON object:
 * {{{
 * {"timestamp":"2023-11-20T10:30:00Z","reason":"...","stackTrace":"...","metadata":{"offset":"..."},"event":{...}}
 * }}}
 *
 * @param filePath The path to the DLQ file. The file and its parent directories will be created if they do not exist.
 * @param toJson A function that serializes an event of type `E` into a JSON string.
 * @tparam E The type of the event to be stored.
 */
class FileBasedDeadLetterQueue[E](filePath: Path)(toJson: E => String) extends DeadLetterQueue[E] {

  // Eagerly create the file and writer to fail fast on permission issues.
  // The writer is configured for append mode and auto-flushing.
  private val writer: PrintWriter = {
    try {
      Option(filePath.getParent).foreach(Files.createDirectories(_))
      new PrintWriter(new FileWriter(filePath.toFile, true), true)
    } catch {
      case NonFatal(e) =>
        System.err.println(
          s"FATAL: Could not open DeadLetterQueue file at $filePath. DLQ will not function."
        )
        e.printStackTrace(System.err)
        // In case of failure, use a writer that does nothing to avoid crashing on every publish.
        new PrintWriter(new StringWriter())
    }
  }

  override def publish(event: E, cause: Throwable, metadata: Map[String, String]): Unit = {
    try {
      val jsonLine = formatAsJsonLine(
        timestamp = Instant.now(),
        reason = cause.toString,
        stackTrace = getStackTraceAsString(cause),
        metadata = metadata,
        event = event
      )
      // The synchronized block ensures that writes from multiple threads are serialized,
      // preventing interleaved content in the output file.
      synchronized {
        writer.println(jsonLine)
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"ERROR: Failed to write event to DeadLetterQueue file at $filePath")
        e.printStackTrace(System.err)
    }
  }

  override def close(): Unit = {
    synchronized {
      writer.close()
    }
  }

  private def getStackTraceAsString(t: Throwable): String = {
    val sw = new StringWriter()
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  private def formatAsJsonLine(
      timestamp: Instant,
      reason: String,
      stackTrace: String,
      metadata: Map[String, String],
      event: E
  ): String = {
    val eventJson = toJson(event)
    val metadataJson = metadata
      .map { case (k, v) =>
        s""""${escapeJsonString(k)}":"${escapeJsonString(v)}""""
      }
      .mkString(",")

    s"""{"timestamp":"$timestamp","reason":"${escapeJsonString(reason)}","stackTrace":"${escapeJsonString(
      stackTrace
    )}","metadata":{$metadataJson},"event":$eventJson}"""
  }

  private def escapeJsonString(text: String): String = {
    val sb = new StringBuilder
    text.foreach {
      case '"' => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\b' => sb.append("\\b")
      case '\f' => sb.append("\\f")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c.isControl => sb.append(f"\\u${c.toInt}%04x")
      case c => sb.append(c)
    }
    sb.toString
  }
}

object FileBasedDeadLetterQueue {

  /**
   * Factory method to create a `FileBasedDeadLetterQueue`.
   *
   * @param filePath The path to the DLQ file.
   * @param toJson A function that serializes an event of type `E` into a JSON string.
   * @tparam E The type of the event.
   * @return A new instance of `FileBasedDeadLetterQueue`.
   */
  def apply[E](filePath: Path)(toJson: E => String): FileBasedDeadLetterQueue[E] =
    new FileBasedDeadLetterQueue(filePath)(toJson)
}