/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

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
