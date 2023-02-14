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
import java.util.UUID
import org.typelevel.ci._
import io.circe.{Decoder, Encoder, Json}
import io.circe.jawn.parse
import io.circe.syntax._
import io.circe.generic.semiauto._
import Common.Error
import cats.Show
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.either._
import cats.syntax.show._
import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.ctl.Common.Error.ServiceError
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client
import org.http4s.{Header, Headers, Request, Response, Uri}
import org.http4s.dsl.io.{DELETE, POST, PUT}

/** Common functions and entities for communication with Iglu Server */
object Server {

  /**
    * Class container holding temporary read/write apikeys, extracted from
    * server response using `getApiKey`
    *
    * @param read stringified UUID for read apikey (not used anywhere)
    * @param write stringified UUID for write apikey
    */
  case class ApiKeys(read: UUID, write: UUID)

  implicit val circeApiKeysDecoder: Decoder[ApiKeys] =
    deriveDecoder[ApiKeys]

  case class HttpUrl(uri: Uri) extends AnyVal {
    override def toString: String = uri.toString
  }

  sealed trait VendorPrefix extends Product with Serializable
  object VendorPrefix {
    case object Wildcard extends VendorPrefix
    case class Concrete(value: String) extends VendorPrefix

    def fromString(s: String): Either[String, VendorPrefix] =
      s.trim match {
        case "*" => Wildcard.asRight
        case other =>
          if (other.matches(VendorRegex)) Concrete(other).asRight
          else s"Vendor prefix $s doesn't match canonical regex ($VendorRegex)".asLeft
      }

    implicit val vendorPrefixShow: Show[VendorPrefix] = Show.show {
      case Wildcard => "*"
      case Concrete(x) => x
    }

    implicit val vendorPrefixCirceEncoder: Encoder[VendorPrefix] =
      Encoder.instance { prefix =>
        Json.fromFields(List("vendorPrefix" -> prefix.show.asJson))
     }
  }

  def createKeys(registryRoot: HttpUrl, masterApiKey: UUID, prefix: VendorPrefix, httpClient: Client[IO]): Failing[ApiKeys] =
    getApiKeys(buildCreateKeysRequest(registryRoot, masterApiKey, prefix), httpClient)

  /**
    * Send DELETE request for an API key.
    *
    * @param key UUID of temporary key
    * @param purpose what exact key being deleted, used to log, can be empty
    */
  def deleteKey(registryRoot: Server.HttpUrl, masterApiKey: UUID, key: UUID, purpose: String, httpClient: Client[IO]): IO[Unit] = {
    val request = Request[IO]()
      .withMethod(DELETE)
      .withUri(registryRoot.uri.addPath("/api/auth/keygen"))
      .withHeaders(Header.Raw(ci"apikey", masterApiKey.toString), Header.Raw(ci"key", key.toString))

    httpClient.run(request).use { response: Response[IO] =>
      response.as[String].map { resp: String =>
        if(response.status.isSuccess) println(s"$purpose key $key deleted")
        else println(s"FAILURE: DELETE $purpose $key response: $resp")
      }.handleErrorWith(throwable => IO.pure(println(s"FAILURE: $purpose $key: ${throwable.toString}")))
    }
  }

  object HttpUrl {
    /**
      * Parse registry root (HTTP URL) from string with default `http://` protocol
      *
      * @param url string representing just host or full URL of registry root.
      *            Registry root is URL **without** /api
      * @return either error or URL tagged as HTTP in case of success
      */
    def parse(url: String): Either[Error, HttpUrl] =
      Either.catchNonFatal {
        if (url.startsWith("http://") || url.startsWith("https://")) {
          HttpUrl(Uri.unsafeFromString(url.stripSuffix("/")))
        } else {
          HttpUrl(Uri.unsafeFromString("http://" + url.stripSuffix("/")))
        }
      }.leftMap(error => Error.ConfigParseError(error.getMessage))
  }

  /**
    * Build HTTP POST-request with master apikey to create temporary
    * read/write apikeys
    *
    * @return HTTP POST-request ready to be sent
    */

  def buildCreateKeysRequest(registryRoot: HttpUrl, masterApiKey: UUID, prefix: VendorPrefix): Request[IO] = {
    Request[IO]()
      .withMethod(POST)
      .withUri(registryRoot.uri.addPath("/api/auth/keygen"))
      .withHeaders(Header.Raw(ci"apikey", masterApiKey.toString))
      .withEntity(prefix)
  }

  /**
    * Build HTTP PUT-request with JSON Schema and authenticated with temporary
    * write key
    *
    * @param schema valid self-describing JSON Schema
    * @param writeKey temporary apikey allowed to write any Schema
    * @return HTTP PUT-request ready to be sent
    */
  def buildPushRequest(registryRoot: HttpUrl, isPublic: Boolean, schema: SelfDescribingSchema[Json], writeKey: UUID) = {
    val uri = registryRoot.uri
      .addPath(s"api/schemas/${schema.self.schemaKey.toPath}")
      .withQueryParam("isPublic", isPublic.toString)

    Request[IO]()
      .withMethod(PUT)
      .withUri(uri)
      .withHeaders(Header.Raw(ci"apikey", writeKey.toString))
      .withEntity(schema)
  }

  /**
    * Build HTTP GET request for all JSON Schemas and authenticated with temporary
    * read key
    *
    * @param registryRoot Iglu server URI
    * @param optReadApiKey optional apikey. If it is None, request will be made without
    *                apikey and therefore only public schemas will be read
    * @return HTTP GET request ready to be sent
    */
  def buildPullRequest(registryRoot: HttpUrl, optReadApiKey: Option[UUID])= {
    val uri = registryRoot.uri
      .addPath("/api/schemas")
      .withQueryParam("repr", "canonical")

    val headers = optReadApiKey match {
      case None => Headers(Header.Raw(ci"accept", "application/json"))
      case Some(readApiKey) => Headers(Header.Raw(ci"accept", "application/json"), Header.Raw(ci"apikey", readApiKey.toString))
    }

    Request[IO]().withUri(uri).withHeaders(headers)
  }

  /**
    * Perform HTTP request bundled with master apikey to create and get
    * temporary read/write apikeys.
    *
    * @param request HTTP request to /api/auth/keygen authenticated by master apikey
    * @return pair of apikeys for successful creation and extraction
    *         error message otherwise
    */

  def getApiKeys(request: Request[IO], httpClient: Client[IO]): Failing[ApiKeys] =
    EitherT(httpClient.run(request).use { response => parseApiKeyResponse(response)})

  def parseApiKeyResponse(response: Response[IO]): IO[Either[Error, ApiKeys]] =
    if (response.status.isSuccess) {
      (for {
        responseString <- EitherT(response.as[String].redeem(err => ServiceError(s"Unable to convert apikey response to string: $err").asLeft, _.asRight))
        jsonOrError <- EitherT(IO.pure(parse(responseString).leftMap(err => Error.fromServer(response, responseString)(err))))
        apiKey <- EitherT(IO.pure(jsonOrError.as[ApiKeys].leftMap(err => Error.fromServer(response, responseString)(err))))
      } yield apiKey).value
    } else response.as[String].map(resp => Left(Error.Message(s"Unexpected status code ${response.status.code}. Response body: ${resp}.")))

  private val VendorRegex = "[a-zA-Z0-9-_.]+"
}
