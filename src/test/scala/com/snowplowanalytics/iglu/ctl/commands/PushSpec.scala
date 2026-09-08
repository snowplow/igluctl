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
package commands

// java
import java.nio.file.Paths
import java.util.UUID

import com.snowplowanalytics.iglu.core.SchemaVer

// cats
import cats.effect.IO
import cats.effect.Ref
import cats.implicits._

// http4s
import org.http4s.{HttpApp, Response, Status}
import org.http4s.client.Client
import org.http4s.implicits._

// circe
import io.circe.literal._

// specs2
import org.specs2.Specification

// This project
import com.snowplowanalytics.iglu.core.SchemaMap
import com.snowplowanalytics.iglu.ctl.File.jsonFile
import com.snowplowanalytics.iglu.ctl.Common.Error
import cats.effect.unsafe.implicits.global


class PushSpec extends Specification { def is = s2"""
  Registry sync command (sync) specification
    check paths on FS and SchemaKey.toPath correspondence $e1
    push schemas to the registry in schema version order $e2
  """

  def e1 = {
    // valid
    val schema1 = json"""
        {
          "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self": {
            "vendor": "com.acme",
            "name": "event",
            "format": "jsonschema",
            "version": "1-0-2"
          },
          "type": "object"
        }"""
    val jsonFile1 = jsonFile(Paths.get("/path/to/schemas/com.acme/event/jsonschema/1-0-2"), schema1)

    ClassLoader.getSystemClassLoader.getResource("")

    // invalid SchemaVer
    val schema2 =  json"""
        {
          "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self": {
            "vendor": "com.acme",
            "name": "event",
            "format": "jsonschema",
            "version": "1-0-1"
          },
          "type": "object"
        }"""
    val jsonFile2 = jsonFile(Paths.get("/path/to/schemas/com.acme/event/jsonschema/1-0-2"), schema2)

    // not self-describing
    val schema3 = json"""
        {
          "type": "object"
        }"""
    val jsonFile3 = jsonFile(Paths.get("/path/to/schemas/com.acme/event/jsonschema/1-0-2"), schema3)

    // not full path
    val schema4 = json"""
        {
          "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self": {
            "vendor": "com.acme",
            "name": "event",
            "format": "jsonschema",
            "version": "1-0-2"
          },
          "type": "object"
        }"""
    val jsonFile4 = jsonFile(Paths.get("/event/jsonschema/1-0-2"), schema4)

    val validSchemaExpectation = jsonFile1.asSchema must beRight
    val mismatchedSchemaVerExpectation = jsonFile2.asSchema must beLeft(Error.PathMismatch(Paths.get("/path/to/schemas/com.acme/event/jsonschema/1-0-2"), SchemaMap("com.acme", "event", "jsonschema", SchemaVer.Full(1,0,1))))
    val invalidSchemaExpectation = jsonFile3.asSchema must  beLeft(Error.ParseError(Paths.get("/path/to/schemas/com.acme/event/jsonschema/1-0-2"), "JSON Schema in file [/path/to/schemas/com.acme/event/jsonschema/1-0-2] is not valid, INVALID_METASCHEMA"))
    val invalidShortPathExpectation = jsonFile4.asSchema must beLeft(Error.PathMismatch(Paths.get("/event/jsonschema/1-0-2"),SchemaMap("com.acme","event","jsonschema",SchemaVer.Full(1,0,2))))

    validSchemaExpectation and mismatchedSchemaVerExpectation and invalidSchemaExpectation and invalidShortPathExpectation
  }

  def e2 = {
    val command = Command.StaticPush(
      input = Paths.get("src/test/resources/unordered-schemas"),
      registryRoot = Server.HttpUrl(uri"http://iglu-server.com"),
      apikey = UUID.fromString("dfa2a4e4-b3f0-4ee5-a4ad-c15b7c0a1b3e"),
      public = false
    )

    val expected = (0 to 11).toList.map(a => s"1-0-$a") ++ List("1-1-0", "2-0-0")

    val pushed = (for {
      recorded <- Ref.of[IO, List[String]](Nil)
      client    = Client.fromHttpApp(HttpApp[IO] { request =>
        recorded
          .update(_ :+ request.uri.path.renderString.split("/").last)
          .as(Response[IO](Status.Created).withEntity("""{"message":"Schema created","location":"iglu:x"}"""))
      })
      _        <- Push.process(command, client).value
      paths    <- recorded.get
    } yield paths).unsafeRunSync()

    pushed must beEqualTo(expected)
  }
}
