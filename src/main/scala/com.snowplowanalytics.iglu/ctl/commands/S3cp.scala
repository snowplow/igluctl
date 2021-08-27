/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.file.{Files, Path}

import cats.data.{ EitherT, NonEmptyList }
import cats.effect.IO
import cats.implicits._

import fs2.Stream

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProvider, DefaultCredentialsProvider, ProfileCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.core.exception.SdkException

import Common.Error

object S3cp {
  def process(inputDir: Path,
              bucketName: String,
              path: Option[String],
              accessKeyId: Option[String],
              secretAccessKey: Option[String],
              profile: Option[String],
              region: Option[String]): Result = {

    val schemasT: Stream[Failing, String] = for {
      s3      <- Stream.eval(getS3(accessKeyId, secretAccessKey, profile, region))
      file    <- File.streamPaths(inputDir).translate[IO, Failing](Common.liftIO).flatMap(Common.liftEither)
      key      = getS3Path(file, inputDir, path)
      result  <- Stream.eval(upload(file, key, s3, bucketName))
    } yield result

    schemasT.compile.toList.leftMap(NonEmptyList.of(_))
  }

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
    * Exception-free upload file to Amazon S3
    *
    * @param file file object ready to be upload on S3. AWS SDK will use its
    *             metadata on S3
    * @param path full path on AWS S3
    * @param service S3 client object
    * @return either error or successful message
    */
  def upload(file: Path, path: String, service: S3Client, bucketName: String): EitherT[IO, Error, String] = {
    EitherT(IO {
      service.putObject(PutObjectRequest.builder.bucket(bucketName).key(path).build, file)
      s"File [${file.toAbsolutePath}] uploaded as [s3://${bucketName + "/" + path}]"
    }.attemptNarrow[SdkException]).leftMap {
        case e: SdkException => Error.ServiceError(e.toString)
    }
  }

  /**
    * Get full S3 path for particular file based on its path of filesystem and
    * specified S3 root
    *
    * @param file file object ready to be upload to S3
    * @return full S3 path
    */
  def getS3Path(file: Path, inputDir: Path, path: Option[String]): String = {
    val pathOnS3: String = {
      val path = file.toAbsolutePath.toString.drop(inputDir.toAbsolutePath.toString.length + 1)
      if (File.separator == """\""")
        path.replace(File.separator, """/""")
      else
        path
    }

    val bucketPath = path match {
      case Some("/") => ""
      case Some(p) if p.endsWith("/") => p
      case Some(p) => p + "/"
      case None => ""
    }

    if (Files.isDirectory(inputDir)) {
      bucketPath + inputDir.getFileName.toString + "/" + pathOnS3
    } else {
      bucketPath + inputDir.getFileName.toString
    }
  }

  /**
   * End-of-the-world class, containing info about success/failure of execution
   *
   * @param successes number of successfully validated schemas
   * @param failures number of schemas with errors
   */
  case class Total(successes: Int, failures: Int) {
    /**
     * Exit from app with error status if invalid schemas were found
     */
    def exit(): Unit = {
      println(s"TOTAL: $successes files were successfully uploaded")
      println(s"TOTAL: $failures errors were encountered")

      if (failures > 0) sys.exit(1)
      else ()
    }

    /**
     * Append and print report for another file
     *
     * @param report file processing result
     * @return modified total object
     */
    def add(report: Either[String, String]): Total = {
      report match {
        case Right(s) =>
          println(s"SUCCESS: $s")
          copy(successes = successes + 1)
        case Left(f) =>
          println(s"FAILURE: $f")
          copy(failures = failures + 1)
      }
    }
  }

  object Total {
    val empty = Total(0, 0)
  }
}
