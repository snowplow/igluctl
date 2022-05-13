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
import java.nio.file.Paths
import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.implicits._
import io.circe.Json

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProvider, DefaultCredentialsProvider, ProfileCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.core.exception.SdkException

import scala.jdk.CollectionConverters._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.core.circe.implicits._

import Common.Error

object S3cp {
  def process(inputDir: Path,
              bucketName: String,
              pathPrefix: Option[String],
              accessKeyId: Option[String],
              secretAccessKey: Option[String],
              profile: Option[String],
              region: Option[String],
              skipSchemaLists: Boolean): Result =
    for {
      s3    <- getS3(accessKeyId, secretAccessKey, profile, region).leftMap(NonEmptyList.of(_))
      files <- EitherT(File.readSchemas(inputDir).map(Common.leftBiasedIor))
      _     <- files.toList.traverse { file =>
        val key = fileToS3Path(Paths.get(file.content.self.schemaKey.toPath), pathPrefix)
        upload(file.content.normalize, key, s3, bucketName)
          .flatMap(url => log(s"File [${file.path.toAbsolutePath}] uploaded as [$url]"))
      }.leftMap(NonEmptyList.of(_))
      _     <- if (skipSchemaLists) EitherT.rightT[IO, NonEmptyList[Common.Error]](Nil)
                else uploadLists(files.toList, pathPrefix, bucketName, s3)
    } yield Nil

  def getS3(accessKeyId: Option[String],
            secretAccessKey: Option[String],
            profile: Option[String],
            region: Option[String]): EitherT[IO, Error, S3Client] = {

    val credentialsProvider = (accessKeyId, secretAccessKey, profile) match {
      case (Some(keyId), Some(secret), None) =>
        EitherT.liftF[IO, Error, AwsCredentialsProvider](IO(StaticCredentialsProvider.create(AwsBasicCredentials.create(keyId, secret))))
      case (None, None, Some(p)) =>
        EitherT.liftF[IO, Error, AwsCredentialsProvider](IO(ProfileCredentialsProvider.create(p)))
      case (None, None, None) =>
        EitherT.liftF[IO, Error, AwsCredentialsProvider](IO(DefaultCredentialsProvider.create))
      case _ =>
        EitherT.leftT[IO, AwsCredentialsProvider](Error.ConfigParseError("Invalid AWS authentication method. Following methods are supported: static credentials, profile, default credentials chain"))
    }

    for {
      provider  <- credentialsProvider
      client <- EitherT.liftF(IO {
        region.foldLeft(S3Client.builder.credentialsProvider(provider)) {
          case (builder, r) => builder.region(Region.of(r))
        }
        .build
      })
    } yield client
  }

  /**
   * Group schemas by vendor/schema/model combination
   *
   * A vendor/schema/model combination is dropped if schemas cannot be unambiguously ordered by when they were published
   */
  def groupByModel(schemaKeys: List[SchemaKey]): FailingNel[Map[Path, List[String]]] = {
    val (unambiguous, ambiguous) =
      schemaKeys
        .groupBy(k => (k.vendor, k.name, k.version.model))
        .toList
        .partition { case (k, schemaKeys) => noAmbiguity(schemaKeys.map(_.version)) }

      ambiguous.traverse { case ((vendor, name, model), _) =>
        log(s"No schema list for $vendor/$name/$model because order is ambiguous")
      }.as {
        unambiguous.map { case ((vendor, name, model), schemaKeys) =>
          Paths.get(vendor, name, "jsonschema", model.toString) -> schemaKeys.map(_.toSchemaUri).sorted
        }.toMap
      }.leftMap(NonEmptyList.of(_))
  }

  /**
   * Predicate for whether schemas can be unambiguously ordered by when they must have been published.
   *
   * "1-0-1", "1-0-2", "1-0-3" should return true
   * "1-0-1", "1-0-1", "1-1-0" should return false
   *
   * It is required that all keys have the same model.
   */
  private def noAmbiguity(keys: List[SchemaVer.Full]): Boolean =
    keys.sortBy(k => (k.revision, k.addition)) === keys.sortBy(k => (k.addition, k.revision))

  def uploadLists(files: List[SchemaFile],
                  pathPrefix: Option[String],
                  bucketName: String,
                  s3: S3Client): FailingNel[Unit] =
    for {
      modelLists <- groupByModel(files.map(_.content.self.schemaKey))
      _          <-uploadModelLists(modelLists, pathPrefix, bucketName, s3)
      _          <-uploadTopLevelList(modelLists, pathPrefix, bucketName, s3)
    } yield ()

  /** Uploads a schema list file for each vendor/schema/model combination */
  def uploadModelLists(modelLists: Map[Path, List[String]],
                       pathPrefix: Option[String],
                       bucketName: String,
                       s3: S3Client): FailingNel[Unit] =
    modelLists.toList.traverse_ { case (path, uris) =>
      val key = fileToS3Path(path, pathPrefix)
      upload(Json.fromValues(uris.map(Json.fromString(_))), key, s3, bucketName)
        .flatMap(url => log(s"Schema list uploaded as [$url]"))
    }.leftMap(NonEmptyList.of(_))

  /** Uploads a top level schema list, corresponds to the /schemas iglu-server api endpoint */
  def uploadTopLevelList(modelLists: Map[Path, List[String]],
                       pathPrefix: Option[String],
                       bucketName: String,
                       s3: S3Client): FailingNel[Unit] = {
    val key = fileToS3Path(Paths.get(""), pathPrefix)
    upload(Json.fromValues(modelLists.toList.flatMap(_._2).map(Json.fromString(_))), key, s3, bucketName)
      .flatMap(url => log(s"Schema list uploaded as [$url]"))
      .leftMap(NonEmptyList.of(_))
  }

  def log(msg: String): Failing[Unit] =
    EitherT.right(IO.delay(println(msg)))

  /**
    * Exception-free upload file to Amazon S3
    *
    * @param json Json content to put in the S3 object
    * @param path full path on AWS S3
    * @param service S3 client object
    * @return either error or url where the file was uploaded
    */
  def upload(json: Json, path: String, service: S3Client, bucketName: String): EitherT[IO, Error, String] = {
    EitherT(IO {
      val content = RequestBody.fromString(json.noSpaces)
      service.putObject(PutObjectRequest.builder.bucket(bucketName).key(path).contentType("application/json; charset=utf-8").build, content)
      s"s3://$bucketName/$path"
    }.attemptNarrow[SdkException]).leftMap {
        case e: SdkException => Error.ServiceError(e.toString)
    }
  }

  /**
    * Get full S3 path for particular file based on its path of filesystem and
    * specified S3 root
    *
    * @param file name of the file object relative to the "schemas" directory
    * @param customPrefix optionally a custom path prefix to use on S3
    * @return full S3 path
    */
  def fileToS3Path(file: Path, customPrefix: Option[String]): String =
    Paths.get(customPrefix.getOrElse(""))
      .resolve(Paths.get("schemas"))
      .resolve(file)
      .asScala.mkString("/")
}
