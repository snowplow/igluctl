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
package com.snowplowanalytics.iglu.ctl

import java.net.URI
import java.util.UUID

import io.circe.{ Decoder, Encoder, Json }
import io.circe.jawn.parse
import io.circe.syntax._
import io.circe.generic.semiauto._

import scalaj.http.{Http, HttpRequest}

import Common.Error

import cats.Show
import cats.data.{ EitherT, Validated }
import cats.syntax.either._
import cats.syntax.show._
import cats.syntax.apply._
import cats.effect.{ IO, Resource }

import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.core.circe.implicits._

/** Common functions and entities for communication with Iglu Server */
object Server {

  /**
    * Class container holding temporary read/write apikeys, extracted from
    * server response using `getApiKey`
    *
    * @param read stringified UUID for read apikey (not used anywhere)
    * @param write stringified UUID for write apikey (not used anywhere)
    */
  case class ApiKeys(read: String, write: String)

  implicit val circeApiKeysDecoder: Decoder[ApiKeys] =
    deriveDecoder[ApiKeys]

  case class HttpUrl(uri: URI) extends AnyVal {
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

  def createKeys(registryRoot: HttpUrl, masterApiKey: UUID, prefix: VendorPrefix): Failing[ApiKeys] =
    for {
      isOldServer <- checkOldServer(registryRoot)
      apiKeys <- getApiKeys(buildCreateKeysRequest(registryRoot, masterApiKey, prefix, isOldServer))
    } yield apiKeys

  /** Create transitive pair of keys that will be deleted right after job completed */
  def temporaryKeys(registryRoot: HttpUrl, masterApiKey: UUID): Resource[Failing, ApiKeys] =
    Resource.make(createKeys(registryRoot, masterApiKey,VendorPrefix.Wildcard)) { keys =>
      val deleteRead = deleteKey(registryRoot, masterApiKey, keys.read, "read")
      val deleteWrite = deleteKey(registryRoot, masterApiKey, keys.write, "write")
      EitherT.liftF(deleteWrite *> deleteRead)
    }

  /**
    * Send DELETE request for an API key.
    *
    * @param key UUID of temporary key
    * @param purpose what exact key being deleted, used to log, can be empty
    */
  def deleteKey(registryRoot: Server.HttpUrl, masterApiKey: UUID, key: String, purpose: String): IO[Unit] = {
    val request = Http(s"$registryRoot/api/auth/keygen")
      .header("apikey", masterApiKey.toString)
      .param("key", key)
      .method("DELETE")

    IO(Validated.catchNonFatal(request.asString) match {
      case Validated.Valid(response) if response.isSuccess => println(s"$purpose key $key deleted")
      case Validated.Valid(response) => println(s"FAILURE: DELETE $purpose $key response: ${response.body}")
      case Validated.Invalid(throwable) => println(s"FAILURE: $purpose $key: ${throwable.toString}")
    })
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
          HttpUrl(new URI(url.stripSuffix("/")))
        } else {
          HttpUrl(new URI("http://" + url.stripSuffix("/")))
        }
      }.leftMap(error => Error.ConfigParseError(error.getMessage))
  }

  /**
    * Build HTTP POST-request with master apikey to create temporary
    * read/write apikeys
    *
    * @param oldServer true if Server is pre-0.6.0
    * @return HTTP POST-request ready to be sent
    */
  def buildCreateKeysRequest(registryRoot: HttpUrl, masterApiKey: UUID, prefix: VendorPrefix, oldServer: Boolean): HttpRequest = {
    val initRequest = Http(s"${registryRoot.uri}/api/auth/keygen").header("apikey", masterApiKey.toString)
    if (oldServer) initRequest.postForm(List(("vendor_prefix", prefix.show)))
    else initRequest.postData(prefix.asJson.noSpaces)
  }

  /**
    * Build HTTP POST-request with JSON Schema and authenticated with temporary
    * write key
    *
    * @param schema valid self-describing JSON Schema
    * @param writeKey temporary apikey allowed to write any Schema
    * @return HTTP POST-request ready to be sent
    */
  def buildRequest(registryRoot: HttpUrl, isPublic: Boolean, schema: SelfDescribingSchema[Json], writeKey: String): HttpRequest =
    Http(s"${registryRoot.uri}/api/schemas/${schema.self.schemaKey.toPath}")
      .header("apikey", writeKey)
      .param("isPublic", isPublic.toString)
      .put(schema.asString)

  /**
    * Perform HTTP request bundled with master apikey to create and get
    * temporary read/write apikeys.
    *
    * @param request HTTP request to /api/auth/keygen authenticated by master apikey
    * @return pair of apikeys for successful creation and extraction
    *         error message otherwise
    */
  def getApiKeys(request: HttpRequest): Failing[ApiKeys] =
    for {
      response  <- EitherT.liftF(IO(request.asString))
      json      <- EitherT.fromEither[IO](parse(response.body)).leftMap(Error.fromServer)
      extracted <- EitherT.fromEither[IO](json.as[ApiKeys]).leftMap(Error.fromServer)
    } yield extracted

  def checkOldServer(registryRoot: HttpUrl): Failing[Boolean] = {
    val healthRequest = Http(s"${registryRoot.uri}/api/meta/health")
    EitherT.liftF(IO(healthRequest.asString)).map(response => !response.body.contains("OK"))
  }

  private val VendorRegex = "[a-zA-Z0-9-_.]+"
}
