package com.snowplowanalytics.iglu.ctl.commands

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.effect.implicits._
import com.dimafeng.testcontainers.MockServerContainer
import com.snowplowanalytics.iglu.ctl.{Command, Common, Server}
import com.snowplowanalytics.iglu.ctl.commands.ITHelpers._
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.specs2.mutable.Specification


class VerifyRedshiftITSpec extends Specification {

  import VerifyRedshiftITSpec._
  
  "verify redshift command" should {
    "return message about breaking schemas when verbose mode is disabled" in {
      val result = process(
        igluSchemas = List(
          testSchema(fields =
          """
              |{"cacheSize": {"type": "integer", "minimum": 0}}
              |""".stripMargin),
          testSchemaWrongType(fields =
            """
              |{"cacheSize": {"type": "number"}}
              |""".stripMargin, 1)
        ),
        verbose = false
      )
      result must beRight(List("iglu:com.test/test/jsonschema/1-*-*"))
    }
    "return message about breaking schemas when verbose mode is enabled" in {
      val result = process(
        igluSchemas = List(
          testSchema(fields =
            """
              |{"cacheSize": {"type": "integer", "minimum": 0}}
              |""".stripMargin),
          testSchemaWrongType(fields =
            """
              |{"cacheSize": {"type": "number"}}
              |""".stripMargin, 1)
        ),
        verbose = true
      )
      result must beRight(List(
        "iglu:com.test/test/jsonschema/1-*-*:",
        "  iglu:com.test/test/jsonschema/1-0-1: [Incompatible types in column cache_size old RedshiftBigInt new RedshiftDouble]"
      ))
    }
    "return message about no breaking changes detected" in {
      val result = process(
        igluSchemas = List(
          testSchema(fields =
            """
              |{"cacheSize": {"type": "number"}}
              |""".stripMargin),
          testSchemaWrongType(fields =
            """
              |{"cacheSize": {"type": "integer"}}
              |""".stripMargin, 1)
        ),
        verbose = false
      )
      result must beRight(List("No breaking changes detected"))
    }
  }

  private def process(igluSchemas: List[String], verbose: Boolean): Either[NonEmptyList[Common.Error], List[String]] = {
    prepareResources
      .use { resources =>
        val command = prepareCommand(resources.igluServer, verbose)
        mockIgluSchemas(resources.igluServer, igluSchemas)
        VerifyRedshift.process(command, resources.httpClient).value
      }.unsafeRunSync()
  }

  private def prepareCommand(igluServer: MockServerContainer, verbose: Boolean) = {
    Command.VerifyRedshift(
      Server.HttpUrl(Uri.unsafeFromString(igluServer.endpoint)),
      None,
      verbose
    )
  }

  private def prepareResources: Resource[IO, TestResources] = {
    for {
      igluServer <- mkContainer(MockServerContainer())
      httpClient <- EmberClientBuilder.default[IO].build
    } yield TestResources(igluServer, httpClient)
  }

}

object VerifyRedshiftITSpec {
  final case class TestResources(igluServer: MockServerContainer, httpClient: Client[IO])
}
