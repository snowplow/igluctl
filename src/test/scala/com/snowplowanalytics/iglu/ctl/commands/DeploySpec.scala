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

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.SchemaVer.Full

import java.nio.file.Paths
import java.net.URI
import java.util.UUID
import io.circe.literal._
import com.snowplowanalytics.iglu.ctl.IgluctlConfig.IgluctlAction
import com.snowplowanalytics.iglu.ctl.commands.Deploy._
import com.snowplowanalytics.iglu.ctl.{Command, IgluctlConfig, Server}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter
import org.http4s.implicits._
import org.specs2.Specification

class DeploySpec extends Specification { def is = s2"""
  Deploy config
    can be parsed successfully $e1
  """

  def e1 = {
    val config = json"""
      {
        "input": "file:///input/path",
        "lint": {
            "includedChecks": ["unknownFormats", "rootObject"],
            "skippedSchemas": [
              "iglu:com.acme/click/jsonschema/1-0-1"
            ]
        },
        "generate": {
            "output": "file:///output/path",
            "withJsonPaths": true,
            "dbschema": "atomic",
            "varcharSize": 4096,
            "noHeader": false,
            "force": false,
            "owner": "a_new_owner"
        },
        "actions": [
            {
                "action": "push",
                "isPublic": true,
                "registry": "http://iglu-server.com",
                "apikey": "79f28002-aadc-4bdf-bd79-7209f28873b9"
            },
            {
                "action": "s3cp",
                "uploadFormat": "jsonschema",
                "bucketPath": "s3://bucket/path_1/path_2",
                "profile": "profile-2",
                "region": "eu-west-2",
                "skipSchemaLists": true
            }
        ]
      }"""
    val inputPath = Paths.get(URI.create("file:///input/path"))
    val outputPath = Paths.get(URI.create("file:///output/path"))
    val expected = IgluctlConfig(
      None,
      Command.Lint(
        inputPath, List(Linter.rootObject, Linter.unknownFormats), List(SchemaKey("com.acme", "click", "jsonschema", Full(1,0,1)))
      ),
      Command.StaticGenerate(
        inputPath, Some(outputPath), "atomic", false
      ),
      List(
        IgluctlAction.Push(Command.StaticPush(
          inputPath, Server.HttpUrl(uri"http://iglu-server.com"),
          UUID.fromString("79f28002-aadc-4bdf-bd79-7209f28873b9"),
          true
        )),
        IgluctlAction.S3Cp(Command.StaticS3Cp(
          inputPath, "bucket", Some("path_1/path_2"),
          None, None, Some("profile-2"), Some("eu-west-2"), true
        ))
      )
    )

    val result = config.as[IgluctlConfig]
    result must beRight(expected)
  }
}
