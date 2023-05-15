/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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

import cats.effect.{ContextShift, IO, Resource, Timer}
import com.dimafeng.testcontainers.{JdbcDatabaseContainer, MockServerContainer, PostgreSQLContainer}
import com.snowplowanalytics.iglu.ctl.commands.TableCheckITSpec._
import com.snowplowanalytics.iglu.ctl.{Command, Server}
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.specs2.mutable.Specification
import org.testcontainers.lifecycle.Startable

import scala.concurrent.ExecutionContext

class TableCheckITSpec extends Specification {

  "Table check" should {
    "report matched table when" >> {
      "all columns and comment match, order not important" in {
        val result = process(
          databaseDefinition = "database/matching-storage.sql",
          igluSchemas = List(testSchema(fields =
            """
              |{
              |		"timestamp": {"type": "string", "format": "date-time"},
              |		"date": {"type": "string", "format": "date"},
              |		"smallint": {"type": "integer", "maximum": 10},
              |		"int": {"type": "integer", "maximum": 1000000},
              |		"bigint": {"type": "integer", "maximum": 100000000000},
              |		"decimal_no_scale": {"type": "integer", "maximum": 0.1},
              |		"decimal_36_2": {"type": "number", "multipleOf": 0.01},
              |		"double": {"type": "number", "multipleOf": 0.001},
              |		"boolean": {"type": "boolean"},
              |		"char": {"type": "string", "minLength": 10, "maxLength": 10},
              |		"varchar": {"type": "string", "maxLength": 10},
              |		"product_4096": {"type": ["string", "number"]},
              |		"product_bool_int": {"type": ["boolean", "integer"]}
              |	}
              |""".stripMargin))
        )
        result must beRight(List(
          """Matched:
            |Table for iglu:com.test/test/jsonschema/1-0-0 is matched
            |----------------------
            |Unmatched: 0, Matched: 1, Not Deployed: 0""".stripMargin
        ))
      }
      "duplicated tables but in different schemas are defined in a database" in {
        process(
          databaseDefinition = "database/duplicated_tables.sql",
          igluSchemas = List(testSchema(fields = """{ "char": {"type": "string"} }"""))
        ) must beRight(List(
          """Matched:
            |Table for iglu:com.test/test/jsonschema/1-0-0 is matched
            |----------------------
            |Unmatched: 0, Matched: 1, Not Deployed: 0""".stripMargin
        ))
      }

    }
    "report unmatched table when" >> {
      "all columns and comment match, order not important, but comment is wrong version" in {
        val result = process(
          databaseDefinition = "database/matching-storage-comment-version.sql",
          igluSchemas = List(testSchema(fields =
            """
              |{
              |		"timestamp": {"type": "string", "format": "date-time"},
              |		"date": {"type": "string", "format": "date"},
              |		"smallint": {"type": "integer", "maximum": 10},
              |		"int": {"type": "integer", "maximum": 1000000},
              |		"bigint": {"type": "integer", "maximum": 100000000000},
              |		"decimal_no_scale": {"type": "integer", "maximum": 0.1},
              |		"decimal_36_2": {"type": "number", "multipleOf": 0.01},
              |		"double": {"type": "number", "multipleOf": 0.001},
              |		"boolean": {"type": "boolean"},
              |		"char": {"type": "string", "minLength": 10, "maxLength": 10},
              |		"varchar": {"type": "string", "maxLength": 10}
              |	}
              |""".stripMargin))
        )
        result must beRight(List(
          """Unmatched:
            |Table for iglu:com.test/test/jsonschema/1-0-0 is not matched. Issues:
            |* Comment problem - SchemaKey found in table comment [iglu:com.test/test/jsonschema/1-0-1] does not match expected [iglu:com.test/test/jsonschema/1-0-0]
            |----------------------
            |Unmatched: 1, Matched: 0, Not Deployed: 0""".stripMargin
        ))
      }
      "there is wrong comment + wrong column type + wrong column nullability + missing and additional column in the storage" in {
        val result = process(
          databaseDefinition = "database/broken-storage.sql",
          igluSchemas = List(testSchema(fields =
            """
              |{
              |   "wrong_type": { "type": "integer" },
              |   "wrong_nullability": { "type": "string" },
              |   "only_in_schema": { "type": "string" }
              |}""".stripMargin))
        )
        result must beRight(List(
          """Unmatched:
            |Table for iglu:com.test/test/jsonschema/1-0-0 is not matched. Issues:
            |* Comment problem - SchemaKey found in table comment [iglu:com.another/test/jsonschema/1-0-0] does not match expected [iglu:com.test/test/jsonschema/1-0-0]
            |* Column doesn't match, expected: 'wrong_type BIGINT', actual: 'wrong_type VARCHAR(4096)'
            |* Column doesn't match, expected: 'wrong_nullability VARCHAR(4096)', actual: 'wrong_nullability VARCHAR(4096) NOT NULL'
            |* Column existing in the storage but is not defined in the schema: 'only_in_storage VARCHAR(4096)'
            |* Column existing in the schema but is not present in the storage: 'only_in_schema VARCHAR(4096)'
            |-----------------
            |Expected columns:
            |'only_in_schema VARCHAR(4096)'
            |'wrong_nullability VARCHAR(4096)'
            |'wrong_type BIGINT'
            |-----------------
            |Existing columns:
            |'only_in_storage VARCHAR(4096)'
            |'wrong_nullability VARCHAR(4096) NOT NULL'
            |'wrong_type VARCHAR(4096)'
            |----------------------
            |Unmatched: 1, Matched: 0, Not Deployed: 0""".stripMargin
        ))
      }
    }
  }

  private def process(databaseDefinition: String,
                      igluSchemas: List[String]) = {
    prepareResources(databaseDefinition)
      .use { resources =>
        val command = prapareCommand(resources.database, resources.igluServer)
        mockIgluSchemas(resources.igluServer, igluSchemas)
        TableCheck.process(command, resources.httpClient).value
      }.unsafeRunSync()
  }

  private def prepareResources(databaseDefinition: String): Resource[IO, TestResources] = {
    for {
      database <- mkContainer(createDatabase(databaseDefinition))
      igluServer <- mkContainer(MockServerContainer())
      httpClient <- EmberClientBuilder.default[IO].build
    } yield TestResources(database, igluServer, httpClient)
  }


  private def prapareCommand(database: PostgreSQLContainer,
                             igluServer: MockServerContainer) = {
    Command.TableCheck(Command.MultipleTableCheck(
      Server.HttpUrl(Uri.unsafeFromString(igluServer.endpoint)), None),
      "atomic",
      Command.DbConfig(
        host = Some("localhost"),
        port = database.mappedPort(5432),
        dbname = Some("test"),
        username = Some("test"),
        password = Some("test")
      )
    )
  }

  private def mkContainer[A <: Startable](container: A): Resource[IO, A] =
    Resource.make {
      IO {
        container.start()
        container
      }
    }(container => IO(container.stop()))


  private def mockIgluSchemas(server: MockServerContainer,
                              igluSchemas: List[String]) = {
    val mockClient = new MockServerClient(server.host, server.serverPort)
    mockClient
      .when(request().withPath("/api/schemas"))
      .respond(response().withStatusCode(200).withBody(igluSchemas.mkString("[", ",", "]")))

  }

  private def createDatabase(initScript: String): PostgreSQLContainer = {
    val params = JdbcDatabaseContainer.CommonParams(initScriptPath = Option(initScript))
    PostgreSQLContainer.Def(
      commonJdbcParams = params
    ).createContainer()
  }

  private def testSchema(fields: String): String =
    s"""
       |{
       |  "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
       |  "description": "Test schema",
       |  "self": {
       |    "vendor": "com.test",
       |    "name": "test",
       |    "format": "jsonschema",
       |    "version": "1-0-0"
       |  },
       |  "type": "object",
       |  "properties": $fields
       |}    
       |""".stripMargin

}

object TableCheckITSpec {

  private val executionContext: ExecutionContext = ExecutionContext.global
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(executionContext)
  implicit val ioTimer: Timer[IO] = IO.timer(executionContext)


  final case class TestResources(database: PostgreSQLContainer,
                                 igluServer: MockServerContainer,
                                 httpClient: Client[IO])

}