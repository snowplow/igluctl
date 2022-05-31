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

import java.nio.file.{Path, Paths}
import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.implicits._
import io.circe.Json
import fs2.Stream
import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.typeclasses.StringifySchema
import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.{Command, Common, Failing, File, Server, Result => IgluctlResult}
import org.http4s.circe.jsonOf
import org.http4s.{Request, Response}
import org.http4s.client.Client


object Pull {

  final implicit val igluStringifySchemaJson: StringifySchema[Json] =
    new StringifySchema[Json] {
      override def asString(container: SelfDescribingSchema[Json]): String =
        container.normalize(igluNormalizeSchemaJson).spaces2
    }
  implicit val listDecoder = jsonOf[IO, List[Json]]

  /**
    * Primary method of static pull command. Fetches all the schemas from given
    * Iglu registry and writes them to given output folder
    */
  def process(command: Command.StaticPull, httpClient: Client[IO]): IgluctlResult = {
    val stream = for {
      schemas <- Stream.eval(getSchemas(Server.buildPullRequest(command.registryRoot, command.apikey), httpClient))
      schema <- Stream.emits[Failing, SelfDescribingSchema[Json]](schemas)
      writeRes <- Stream.eval(writeSchema(schema, command.output))
    } yield writeRes

    stream.compile.toList.leftMap(e => NonEmptyList.of(e))
  }

  /**
    * Perform HTTP request bundled with temporary read key to read all schemas
    * @param request HTTP GET-request to read all schemas from Iglu Server
    * @return List of SelfDescribingSchema which fetched from Iglu Server or
    *         error message
    */
  def getSchemas(request: Request[IO], httpClient: Client[IO]): Failing[List[SelfDescribingSchema[Json]]] = {
    val result = httpClient.run(request).use { response =>
      parseResponse(response)
    }
    EitherT(result)
  }

  /**
    * Extract list of SelfDescribingSchema from server response
    * @param response HTTP response from Iglu registry, presumably containing JSON
    * @return List of SelfDescribingSchema or error message if fetching
    *         wasn't successful
    */
  private[ctl] def parseResponse(response: Response[IO]): IO[Either[Common.Error, List[SelfDescribingSchema[Json]]]] = {
    def parsingError(msg: String): Error.Message = Common.Error.Message(s"Error while parsing response: ${msg}")
    if(response.status.isSuccess){
      response.as[List[Json]]
        .redeem(e => Left(parsingError(e.getMessage)), schemas => schemas.traverse(SelfDescribingSchema.parse[Json])
        .leftMap(e => parsingError(e.toString)))
    } else response.as[String].map(resp => Left(Common.Error.Message(s"Unexpected status code ${response.status.code}. Response body: ${resp}.")))
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
