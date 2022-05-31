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

import java.util.UUID
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{IO, Resource}
import cats.implicits._
import com.snowplowanalytics.iglu.ctl.Command.StaticPush
import com.snowplowanalytics.iglu.ctl.{Result => IgluctlResult}
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.http4s.circe.jsonOf
import org.http4s.client.Client
import org.http4s.{EntityDecoder, Request, Response}

/**
 * Companion objects, containing functions not closed on `masterApiKey`, `registryRoot`, etc
 */
object Push {

  /**
    * Primary function, performing IO reading, processing and printing results
    * @param inputDir path to schemas to upload
    * @param registryRoot Iglu Server endpoint (without `/api`)
    * @param apiKey API key with write permissions (master key if `legacy` is true)
    * @param isPublic whether schemas should be publicly available
    * @param legacy whether it should be compatible with pre-0.6.0 Server,
    *               which required to create temporary keys first
    */
  def process(command: StaticPush, httpClient: Client[IO]): IgluctlResult = {
    val apiKeyResource =
      if(command.legacy)
        Server.temporaryKeys(command.registryRoot, command.apikey, httpClient).map(_.write)
      else
        Resource.pure[Failing, UUID](command.apikey)

    apiKeyResource.mapK[Failing, FailingNel](Common.liftFailingNel).use { apiKey =>
      for {
        files   <- EitherT(File.readSchemas(command.input).map(Common.leftBiasedIor))
        result  <- files.toList.traverse { file =>
          val request = Server.buildPushRequest(command.registryRoot, command.public, file.content, apiKey)
          postSchema(request, httpClient).map(_.asString)
        }.leftMap(NonEmptyList.of(_))
      } yield result
    }
  }

  /**
   * Common server message extracted from HTTP JSON response
   *
   * @param status HTTP status code
   * @param message human-readable message
   * @param location optional URI available for successful upload
   */
  case class ServerMessage(status: Option[Int], message: String, location: Option[String])

  object ServerMessage {
    def asString(status: Option[Int], message: String, location: Option[String]): String =
      s"$message ${location.map("at " + _ + " ").getOrElse("")} ${status.map(x => s"($x)").getOrElse("")}"

    implicit val serverMessageCirceDecoder: Decoder[ServerMessage] =
      deriveDecoder[ServerMessage]
    implicit val serverMessageDecoder: EntityDecoder[IO, ServerMessage] = jsonOf[IO, ServerMessage]
  }

  /**
   * ADT representing all possible statuses for Schema upload
   */
  sealed trait Status extends Serializable

  object Status {
    case object Updated extends Status
    case object Created extends Status
    case object Unknown extends Status
    case object Failed extends Status
  }

  /**
   * Final result of uploading schema, with server response or error message
   *
   * @param serverMessage message, represented as valid [[ServerMessage]]
   *                      extracted from response or plain string if response
   *                      was unexpected
   * @param status short description of outcome
   */
  case class Result(serverMessage: Either[String, ServerMessage], status: Status) {
    def asString: String =
      serverMessage match {
        case Right(message) => ServerMessage.asString(message.status, message.message, message.location)
        case Left(responseBody) => responseBody
      }
  }

  /**
    * Perform HTTP request bundled with temporary write key and valid
    * self-describing JSON Schema to /api/schemas/SCHEMAPATH to publish new
    * Schema.
    * Performs IO
    *
    * @param request HTTP POST-request with JSON Schema
    * @return successful parsed message or error message
    */
  def postSchema(request: Request[IO], httpClient: Client[IO]): Failing[Result] = {
    val result = httpClient.run(request).use { response: Response[IO] =>
      if(response.status.isSuccess) {
          response.as[ServerMessage].map { msg =>
            if (msg.message.contains("updated"))
              Result(Right(msg), Status.Updated)
            else
              Result(Right(msg), Status.Created)
        }.handleErrorWith(err => IO.pure(Result(Left(err.getMessage), Status.Unknown)))
      } else {
        response.as[String].map(s => Result(Left(s), Status.Failed))
      }
    }
    EitherT.liftF(result)
  }

}
