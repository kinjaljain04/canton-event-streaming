/*
 * Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.digitalasset.canton.integration.tool.streaming

import com.daml.ledger.api.v1.value.{Identifier, Record, Value}
import scala.util.Try

/**
 * A Domain-Specific Language (DSL) for filtering Daml ledger events.
 *
 * This allows for flexible routing and processing of `CreatedEvent` data based on
 * template IDs and contract argument values. Filters can be combined using
 * logical operators (AND, OR, NOT) to build complex rules.
 *
 * The primary entry point for matching is the `matches` method, which takes the
 * template ID and payload of a created event.
 */
sealed trait EventFilter extends Product with Serializable {

  /**
   * Evaluates whether a given ledger event matches the filter criteria.
   *
   * @param templateId The `Identifier` of the contract template.
   * @param payload    The `Record` containing the contract arguments (payload).
   * @return `true` if the event matches the filter, `false` otherwise.
   */
  def matches(templateId: Identifier, payload: Record): Boolean
}

object EventFilter {

  /** A filter that accepts all events. */
  case object AcceptAll extends EventFilter {
    override def matches(templateId: Identifier, payload: Record): Boolean = true
  }

  /** A filter that rejects all events. */
  case object RejectAll extends EventFilter {
    override def matches(templateId: Identifier, payload: Record): Boolean = false
  }

  /**
   * A composite filter that matches if and only if all of its sub-filters match.
   *
   * @param filters A sequence of `EventFilter`s to be combined with AND logic.
   *                An empty sequence is vacuously true (matches all events).
   */
  case class And(filters: Seq[EventFilter]) extends EventFilter {
    override def matches(templateId: Identifier, payload: Record): Boolean = {
      filters.forall(_.matches(templateId, payload))
    }
  }

  /**
   * A composite filter that matches if at least one of its sub-filters match.
   *
   * @param filters A sequence of `EventFilter`s to be combined with OR logic.
   *                An empty sequence is vacuously false (matches no events).
   */
  case class Or(filters: Seq[EventFilter]) extends EventFilter {
    override def matches(templateId: Identifier, payload: Record): Boolean = {
      filters.exists(_.matches(templateId, payload))
    }
  }

  /**
   * A filter that inverts the result of another filter.
   *
   * @param filter The `EventFilter` whose result will be negated.
   */
  case class Not(filter: EventFilter) extends EventFilter {
    override def matches(templateId: Identifier, payload: Record): Boolean = {
      !filter.matches(templateId, payload)
    }
  }

  /**
   * A filter that matches events based on a set of allowed template IDs.
   *
   * @param templateIds A set of `Identifier`s. An event matches if its template ID is in this set.
   */
  case class ByTemplate(templateIds: Set[Identifier]) extends EventFilter {
    override def matches(templateId: Identifier, payload: Record): Boolean = {
      templateIds.contains(templateId)
    }
  }

  /**
   * A filter that matches events based on the value of a specific contract argument.
   *
   * @param path      A list of strings representing the path to a nested field within the payload.
   *                  For example, `List("deal", "amount")` would access `payload.deal.amount`.
   * @param predicate The `ValuePredicate` to apply to the field's value.
   */
  case class ByArgument(path: List[String], predicate: ValuePredicate) extends EventFilter {
    override def matches(templateId: Identifier, payload: Record): Boolean = {
      val value = EventFilter.extractValue(payload, path)
      predicate.eval(value)
    }
  }

  /**
   * Recursively extracts a `Value` from a `Record` given a path of field labels.
   *
   * @param record The record to search within.
   * @param path   The list of field labels representing the path to the desired value.
   * @return `Some(Value)` if the path is valid and a value is found, `None` otherwise.
   */
  private[streaming] def extractValue(record: Record, path: List[String]): Option[Value] = {
    path match {
      case Nil => Some(Value(Value.Sum.Record(record)))
      case head :: tail =>
        record.fields.find(_.label == head).flatMap { field =>
          field.value match {
            case Some(v) =>
              tail match {
                case Nil => Some(v) // Found the target value
                case nextPath =>
                  v.sum match {
                    // Recurse into nested record
                    case Value.Sum.Record(nestedRecord) => extractValue(nestedRecord, nextPath)
                    // Path continues, but value is not a container
                    case _ => None
                  }
              }
            case None => None // Field exists but has no value
          }
        }
    }
  }

  /**
   * Represents a predicate to be applied to a Daml `Value`.
   * Used by `ByArgument` filter to perform comparisons and checks.
   */
  sealed trait ValuePredicate extends Product with Serializable {

    /**
     * Evaluates the predicate against an optional `Value`.
     * The value is optional because the path in `ByArgument` may not exist.
     *
     * @param optValue The `Option[Value]` to test.
     * @return `true` if the predicate is satisfied, `false` otherwise.
     */
    def eval(optValue: Option[Value]): Boolean
  }

  object ValuePredicate {

    /** A predicate that checks for the existence of a field at a given path. */
    case object Exists extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean = optValue.isDefined
    }

    /** A predicate that checks if a field's value is equal to an expected value. */
    case class IsEqualTo(expected: Value) extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean = optValue.contains(expected)
    }

    /** A predicate that checks if a Daml Text value is equal to a given String. */
    case class TextIs(expected: String) extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean =
        optValue.exists(v => v.sum.isText && v.getText == expected)
    }

    /** A predicate that checks if a Daml Party value is equal to a given String. */
    case class PartyIs(expected: String) extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean =
        optValue.exists(v => v.sum.isParty && v.getParty == expected)
    }

    /** A predicate that checks if a Daml Numeric value is greater than a given threshold. */
    case class NumericGt(threshold: BigDecimal) extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean = {
        optValue.exists { v =>
          v.sum.isNumeric && Try(BigDecimal(v.getNumeric) > threshold).getOrElse(false)
        }
      }
    }

    /** A predicate that checks if a Daml Numeric value is less than a given threshold. */
    case class NumericLt(threshold: BigDecimal) extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean = {
        optValue.exists { v =>
          v.sum.isNumeric && Try(BigDecimal(v.getNumeric) < threshold).getOrElse(false)
        }
      }
    }

    /** A predicate that checks if a Daml Int64 value is equal to a given Long. */
    case class Int64Is(expected: Long) extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean =
        optValue.exists(v => v.sum.isInt64 && v.getInt64 == expected)
    }

    /** A predicate that checks if a Daml Bool value is equal to a given Boolean. */
    case class BoolIs(expected: Boolean) extends ValuePredicate {
      override def eval(optValue: Option[Value]): Boolean =
        optValue.exists(v => v.sum.isBool && v.getBool == expected)
    }
  }
}