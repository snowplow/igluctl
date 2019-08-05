package com.snowplowanalytics.iglu.ctl

import cats.data.NonEmptyList
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaVer}
import com.snowplowanalytics.iglu.ctl.Common.GapError.{Gaps, InitMissing}
import org.specs2.Specification

class CommonSpec extends Specification { def is = s2"""
  Check util functions in Common
    checkSchemasConsistency return GapError list when there are gap errors $e1
    checkSchemasConsistency return unit right when there is no gap error $e2
  """

  def e1 = {
    val schemas = NonEmptyList.of(
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 0)),
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 3)),
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(2, 0, 0)),
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(2, 0, 1)),

      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(1, 0, 0)),
      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(1, 0, 3)),
      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(2, 0, 0)),
      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(2, 0, 1)),

      SchemaMap("vendor1", "name3", "jsonschema", SchemaVer.Full(1, 0, 2)),
      SchemaMap("vendor1", "name3", "jsonschema", SchemaVer.Full(2, 0, 0)),
      SchemaMap("vendor1", "name3", "jsonschema", SchemaVer.Full(2, 0, 1)),

      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(1, 0, 0)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(1, 0, 1)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 0)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 1)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 2)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 4)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 5)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 6)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 8)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 9)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 10)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 11))
    )

    val res = Common.checkSchemasConsistency(schemas).leftMap(_.toList.toSet)
    val expected = Set(InitMissing("vendor1", "name3"), Gaps("vendor1", "name1"), Gaps("vendor1", "name2"), Gaps("vendor2", "name1"))
    res must beEqualTo(Left(expected))
  }

  def e2 = {
    val schemas = NonEmptyList.of(
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 0)),
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 1)),
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(2, 0, 0)),
      SchemaMap("vendor1", "name1", "jsonschema", SchemaVer.Full(2, 0, 1)),

      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(1, 0, 0)),
      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(1, 0, 1)),
      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(2, 0, 0)),
      SchemaMap("vendor1", "name2", "jsonschema", SchemaVer.Full(2, 0, 1)),

      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(1, 0, 0)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(1, 0, 1)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 0)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 1)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 2)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 3)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 4)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 5)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 6)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 7)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 8)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 9)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 10)),
      SchemaMap("vendor2", "name1", "jsonschema", SchemaVer.Full(2, 0, 11))
    )

    Common.checkSchemasConsistency(schemas) must beRight
  }
}
