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

import java.nio.file.Path
import java.util.UUID

import cats.data.{EitherT, NonEmptyList}
import cats.effect.{IO, Resource}
import cats.implicits._

import scalaj.http.{HttpRequest, HttpResponse}

import io.circe.Decoder
import io.circe.jawn.parse
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.ctl.{ Result => IgluctlResult }

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
  def process(inputDir: Path,
              registryRoot: Server.HttpUrl,
              apiKey: UUID,
              isPublic: Boolean,
              legacy: Boolean): IgluctlResult = {
    val apiKeyResource: Resource[Failing, UUID] = if (legacy) Server.temporaryKeys(registryRoot, apiKey).map(_.write) else Resource.pure[Failing, UUID](apiKey)

    apiKeyResource.mapK[Failing, FailingNel](Common.liftFailingNel).use { apiKey =>
      for {
        files   <- EitherT(File.readSchemas(inputDir).map(Common.leftBiasedIor))
        result  <- files.toList.traverse { file =>
            val request = Server.buildPushRequest(registryRoot, isPublic, file.content, apiKey)
            postSchema(request).map(_.asString)
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
   * Transform failing [[Result]] to plain [[Result]] by inserting exception
   * message instead of server message
   *
   * @param result disjucntion of string with result
   * @return plain result
   */
  def flattenResult(result: Either[String, Result]): Result =
    result match {
      case Right(status) => status
      case Left(failure) => Result(Left(failure), Status.Failed)
    }

  /**
   * Extract stringified message from server response through [[ServerMessage]]
   *
   * @param response HTTP response from Iglu registry, presumably containing JSON
   * @return success message processed from JSON or error message if upload
   *         wasn't successful
   */
  def getUploadStatus(response: HttpResponse[String]): Result = {
    if (response.isSuccess) {
      val result = for {
        json <- parse(response.body)
        message <- json.as[ServerMessage]
      } yield message

      result match {
        case Right(serverMessage) if serverMessage.message.contains("updated") =>
          Result(Right(serverMessage), Status.Updated)
        case Right(serverMessage) =>
          Result(Right(serverMessage), Status.Created)
        case Left(_) =>
          Result(Left(response.body), Status.Unknown)
      }
    } else Result(Left(response.body), Status.Failed)
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
  def postSchema(request: HttpRequest): Failing[Result] =
    EitherT.liftF(IO(request.asString).map(getUploadStatus))
}
