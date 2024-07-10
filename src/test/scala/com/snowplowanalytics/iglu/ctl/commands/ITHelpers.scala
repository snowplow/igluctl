package com.snowplowanalytics.iglu.ctl.commands

import cats.effect._
import cats.effect.implicits._
import com.dimafeng.testcontainers.MockServerContainer
import org.mockserver.client.MockServerClient
import org.mockserver.mock.Expectation
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.testcontainers.lifecycle.Startable

import scala.concurrent.ExecutionContext

object ITHelpers {

  val executionContext: ExecutionContext = ExecutionContext.global
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(executionContext)
  implicit val ioTimer: Timer[IO] = IO.timer(executionContext)

  def testSchema(fields: String): String =
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

  def testSchemaWrongType(fields: String, minor: Int): String =
    s"""
       |{
       |  "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
       |  "description": "Test schema",
       |  "self": {
       |    "vendor": "com.test",
       |    "name": "test",
       |    "format": "jsonschema",
       |    "version": "1-0-$minor"
       |  },
       |  "type": "object",
       |  "properties": $fields
       |}
       |""".stripMargin

  def mkContainer[A <: Startable](container: A): Resource[IO, A] =
    Resource.make {
      IO {
        container.start()
        container
      }
    }(container => IO(container.stop()))


  def mockIgluSchemas(server: MockServerContainer, igluSchemas: List[String]): Array[Expectation] = {
    val mockClient = new MockServerClient(server.host, server.serverPort)
    mockClient
      .when(request().withPath("/api/schemas"))
      .respond(response().withStatusCode(200).withBody(igluSchemas.mkString("[", ",", "]")))
  }
}
