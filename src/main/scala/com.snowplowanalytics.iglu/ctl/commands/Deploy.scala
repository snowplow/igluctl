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

import java.nio.file.{Path, Paths}
import java.util.UUID
import java.net.URI

import cats.data.{EitherT, NonEmptyList}
import cats.effect._
import cats.implicits._

import com.typesafe.config.{ConfigFactory, Config => RawConfig}

import io.circe._
import io.circe.config.parser

import com.snowplowanalytics.iglu.ctl.File.readFile
import com.snowplowanalytics.iglu.ctl.IgluctlConfig.IgluctlAction
import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.commands.Deploy.ApiKeySecret.EnvVar
import com.snowplowanalytics.iglu.ctl.Command.StaticDeploy

import org.http4s.client.Client

object Deploy {

  private val tempPath = Paths.get("/temp")

  /**
    * Primary method of static deploy command
    * Performs usual schema workflow at once, per configuration file
    * Short-circuits on first failed step
    */
  def process(command: StaticDeploy, client: Client[IO]): Result = {
    for {
      configDoc    <- EitherT(readFile(command.config)).leftMap(NonEmptyList.of(_))
      cfg          <- EitherT.fromEither[IO](parseConfig(configDoc.content)).leftMap(e => NonEmptyList.of(Error.ConfigParseError(e)))
      output       <- Lint.process(cfg.lint)
      _            <- Generate.process(cfg.generate)
      actionsOut   <- cfg.actions.traverse[EitherT[IO, NonEmptyList[Common.Error], *], List[String]](IgluctlConfig.process(client, _))
    } yield output ::: actionsOut.flatten
  }

  private def parseConfig(text: String): Either[String, IgluctlConfig] =
    for {
      raw <- Either
        .catchNonFatal(ConfigFactory.parseString(text).resolve)
        .leftMap(_.toString)
      raw <- optionallySdj(raw).asRight
      parsed <- parser
        .decode[IgluctlConfig](raw)
        .leftMap(e => s"Could not parse config: ${e.show}")
    } yield parsed

  /**
   * Accept the configuration as either regular hocon or as self-describing json.
   *
   * This is included for legacy reasons: igluctl was previously configured with SDJ only but now we
   * prefer hocon. The SDJ parsing is lax: we do not validate the metadata.
   */
  private def optionallySdj(config: RawConfig): RawConfig =
    if (config.hasPath("data"))
      config.getConfig("data")
    else
      config

  /** Configuration key that can represent either "hard-coded" value or env var with API key */
  private[ctl] sealed trait ApiKeySecret {
    def value: Either[Error, UUID]
  }

  private[ctl] object ApiKeySecret {
    case class Plain(uuid: UUID) extends ApiKeySecret {
      def value: Either[Error, UUID] = uuid.asRight
    }
    case class EnvVar(name: String) extends ApiKeySecret {
      def value: Either[Error, UUID] =
        for {
          value <- Option(System.getenv().get(name)).toRight(Error.ConfigParseError(s"Environment variable $name is not available"))
          uuid <- Either.catchNonFatal(UUID.fromString(value)).leftMap(e => Error.ConfigParseError(e.getMessage))
        } yield uuid
    }
  }

  implicit val igluCtlConfigDecoder: Decoder[IgluctlConfig] =
    Decoder.instance { cursor =>
      for {
        description <- cursor.downField("description").as[Option[String]]
        input       <- cursor.downField("input").as[Path]
        lint        <- cursor.downField("lint").as[Command.Lint]
        generate    <- cursor.downField("generate").as[Command.StaticGenerate]
        actions     <- cursor.downField("actions").as[List[IgluctlAction]]
      } yield IgluctlConfig(
        description,
        lint.copy(input = input),
        generate.copy(input = input),
        actions.map {
          case IgluctlAction.Push(command) => IgluctlAction.Push(command.copy(input = input))
          case IgluctlAction.S3Cp(command) => IgluctlAction.S3Cp(command.copy(input = input))
        }
      )
    }

  implicit val lintConfigDecoder: Decoder[Command.Lint] =
    Decoder.instance { cursor =>
      for {
        includedChecks <- cursor.downField("includedChecks").as[List[String]]
        skippedSchemas <- cursor.downField("skippedSchemas").as[List[String]]
        linters <- Lint.parseOptionalLinters(includedChecks.mkString(",")).leftMap(e => DecodingFailure(e.toString, Nil))
        schemas <-
          if(skippedSchemas.isEmpty)
            Nil.asRight[DecodingFailure]
          else
            Lint.parseSkippedSchemas(skippedSchemas.mkString(",")).leftMap(e => DecodingFailure(e.toString, Nil))
      } yield Command.Lint(tempPath, linters, schemas)
    }

  implicit val generateConfigDecoder: Decoder[Command.StaticGenerate] =
    Decoder.instance { cursor =>
      for {
        output        <- cursor.downField("output").as[Path]
        dbSchema      <- cursor.downField("dbschema").as[String]
        usePostgres   <- cursor.downField("usepostgres").as[Boolean]
        force         <- cursor.downField("force").as[Boolean]
      } yield Command.StaticGenerate(tempPath, Some(output), dbSchema, usePostgres, force)
    }

  implicit val igluCtlActionDecoder: Decoder[IgluctlAction] =
    Decoder.instance { cursor =>
      cursor.downField("action").as[String] match {
        case Right("s3cp") =>
          for {
            bucket     <- cursor.downField("bucketPath").as[String]
            profile    <- cursor.downField("profile").as[String]
            region     <- cursor.downField("region").as[String]
            skipSchemaLists <- cursor.getOrElse[Boolean]("skipSchemaLists")(false)
            bucketPath <- bucket.stripPrefix("s3://").split("/").toList match {
              case b :: Nil => (b, None).asRight
              case b :: p => (b, Some(p.mkString("/"))).asRight
              case _ => DecodingFailure("bucketPath has invalid format", cursor.history).asLeft
            }
            (b, p) = bucketPath
          } yield IgluctlAction.S3Cp(Command.StaticS3Cp(tempPath, b, p, None, None, Some(profile), Some(region), skipSchemaLists))
        case Right("push") =>
          for {
            registryRoot <- cursor.downField("registry").as[Server.HttpUrl]
            secret       <- cursor.downField("apikey").as[ApiKeySecret]
            masterApiKey <- secret.value.leftMap(e => DecodingFailure(e.toString, cursor.history))
            isPublic     <- cursor.downField("isPublic").as[Boolean]
          } yield IgluctlAction.Push(Command.StaticPush(tempPath, registryRoot, masterApiKey, isPublic))
        case Right(unknown) =>
          DecodingFailure(s"Unknown IgluCtl action: $unknown", cursor.history).asLeft
        case Left(e) =>
          e.asLeft
      }
    }

  implicit val serverHttpUrlDecoder: Decoder[Server.HttpUrl] =
    Decoder.instance { cursor =>
      cursor.as[String].flatMap { url =>
        Server.HttpUrl
          .parse(url)
          .leftMap { e =>
            DecodingFailure(e.toString, cursor.history)
          }
      }
    }

  implicit val apiKeySecretDecoder: Decoder[ApiKeySecret] =
    Decoder.instance { cursor =>
      cursor.as[String].flatMap {
        case value if value.startsWith("$") => EnvVar(value.drop(1)).asRight
        case value => Either.catchNonFatal(UUID.fromString(value))
          .map(uuid => ApiKeySecret.Plain(uuid))
          .leftMap(e => DecodingFailure(s"apikey can have ENVVAR or UUID format; ${e.getMessage}", cursor.history))
      }
    }

  implicit val javaPathCirceDecoder: Decoder[Path] =
    Decoder.instance { cursor =>
      cursor.as[String].flatMap { uriString =>
        val uriStringWithFile = if (!uriString.startsWith("file")) "file://" ++ uriString else uriString
        Either.catchNonFatal(Paths.get(URI.create(uriStringWithFile)))
          .leftMap(e => DecodingFailure(s"Error while creating path: $e", cursor.history))
      }
    }

}
