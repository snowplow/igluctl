/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.file.{Path, Paths}
import java.util.UUID

import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.implicits._

import io.circe.jawn.parse
import io.circe.Json

import fs2.Stream

import scalaj.http.{HttpRequest, HttpResponse}

import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.typeclasses.StringifySchema

import com.snowplowanalytics.iglu.ctl.{Common, Failing, Server, Result => IgluctlResult}
import com.snowplowanalytics.iglu.ctl.File


object Pull {

  final implicit val igluStringifySchemaJson: StringifySchema[Json] =
    new StringifySchema[Json] {
      override def asString(container: SelfDescribingSchema[Json]): String =
        container.normalize(igluNormalizeSchemaJson).spaces2
    }

  /**
    * Primary method of static pull command. Fetches all the schemas from given
    * Iglu registry and writes them to given output folder
    */
  def process(outputDir: Path,
              registryRoot: Server.HttpUrl,
              readApiKey: Option[UUID]): IgluctlResult = {
    val stream = for {
      schemas <- Stream.eval(getSchemas(Server.buildPullRequest(registryRoot, readApiKey)))
      schema <- Stream.emits[Failing, SelfDescribingSchema[Json]](schemas)
      writeRes <- Stream.eval(writeSchema(schema, outputDir))
    } yield writeRes

    stream.compile.toList.leftMap(e => NonEmptyList.of(e))
  }

  /**
    * Perform HTTP request bundled with temporary read key to read all schemas
    * @param request HTTP GET-request to read all schemas from Iglu Server
    * @return List of SelfDescribingSchema which fetched from Iglu Server or
    *         error message
    */
  def getSchemas(request: HttpRequest): Failing[List[SelfDescribingSchema[Json]]] =
    Common.liftIO(IO(request.asString))
      .flatMap(r => EitherT.fromEither(parseResponse(r)))

  /**
    * Extract list of SelfDescribingSchema from server response
    * @param response HTTP response from Iglu registry, presumably containing JSON
    * @return List of SelfDescribingSchema or error message if fetching
    *         wasn't successful
    */
  private[ctl] def parseResponse(response: HttpResponse[String]): Either[Common.Error, List[SelfDescribingSchema[Json]]] = {
    if (response.isSuccess) {
      val result = for {
        json <- parse(response.body)
        schemaJson <- json.as[List[Json]]
      } yield schemaJson

      result
        .flatMap(_.traverse(SelfDescribingSchema.parse[Json]))
        .leftMap(e => Common.Error.Message(s"Error while parsing response: ${e.toString}"))

    } else Left(Common.Error.Message(s"Unexpected status code ${response.code}. Response body: ${response.body}."))
  }

  /**
    * Write SelfDescribingSchema as file
    * @param schema SelfDescribingSchema to write to file
    * @param outputDir The path to write to
    * @return File writing response
    */
  private def writeSchema(schema: SelfDescribingSchema[Json], outputDir: Path): EitherT[IO, Common.Error, String] = {
    val file = File.textFile(Paths.get(s"$outputDir/${schema.self.schemaKey.toPath}"), schema.asString)
    EitherT(file.write(true))
  }
}
