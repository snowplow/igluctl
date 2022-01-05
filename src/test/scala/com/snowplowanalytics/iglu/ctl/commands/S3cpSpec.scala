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
package com.snowplowanalytics.iglu.ctl.commands

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import org.specs2.mutable.Specification

import java.nio.file.Paths

class S3cpSpec extends Specification {

  "fileToS3Path" >> {
    "should resolve file names with no custom prefix" >> {
      val path = Paths.get("com.acme", "myschema", "jsonschema", "1-0-0")
      val result = S3cp.fileToS3Path(path, None)

      result must_== "schemas/com.acme/myschema/jsonschema/1-0-0"
    }

    "should resolve top level path with no custom prefix" >> {
      val path = Paths.get("")
      val result = S3cp.fileToS3Path(path, None)

      result must_== "schemas"
    }

    "should resolve file names with a custom prefix" >> {
      val path = Paths.get("com.acme", "myschema", "jsonschema", "1-0-0")
      List("myprefix", "/myprefix", "myprefix/").map { prefix =>
        val result = S3cp.fileToS3Path(path, Some(prefix))

        result must_== "myprefix/schemas/com.acme/myschema/jsonschema/1-0-0"
      }
    }
  }

  "groupByModel" >> {
    "should return grouped schemas with no ambiguities" >> {
      val input = List(
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 0)),
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 1)),
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 2)),
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(2, 0, 0)),
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(2, 1, 0)),
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(2, 2, 0)),
        )

      val expected = Map(
        Paths.get("vendor1", "name1", "jsonschema", "1") -> List(
            "iglu:vendor1/name1/jsonschema/1-0-0",
            "iglu:vendor1/name1/jsonschema/1-0-1",
            "iglu:vendor1/name1/jsonschema/1-0-2"
          ),
        Paths.get("vendor1", "name1", "jsonschema", "2") -> List(
            "iglu:vendor1/name1/jsonschema/2-0-0",
            "iglu:vendor1/name1/jsonschema/2-1-0",
            "iglu:vendor1/name1/jsonschema/2-2-0",
          )
      )

      S3cp.groupByModel(input).value.unsafeRunSync() must beRight(expected)

    }

    "should drop schemas with ambiguous sort order" >> {
      val input = List(
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 0)),
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 0, 1)),
        SchemaKey("vendor1", "name1", "jsonschema", SchemaVer.Full(1, 1, 0))
        )

      S3cp.groupByModel(input).value.unsafeRunSync() must_== Right(Map.empty)

    }
  }
}
