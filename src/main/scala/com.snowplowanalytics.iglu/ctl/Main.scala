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

import cats.Show
import cats.data.{EitherNel, EitherT}
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.snowplowanalytics.iglu.ctl.commands._
import com.snowplowanalytics.iglu.ctl.Common.Error
import org.http4s.client.Client
import org.http4s.client.middleware.FollowRedirect
import org.http4s.ember.client.EmberClientBuilder


object Main extends IOApp {

  // Check -> Folder does not exist. Common. Input folder presence is checked in Main

  // generate
  // Read -> (inaccessible, not a JSON, not a Schema) -- filter out a file entirely, but do not short-circuit the process
  // Read -> (incompatible path) -- short-circuit the process
  // Read -> load everything into memory
  // Lint -> on very permissive set of checks (only errors)
  // Write -> short circuits at any point
  // Write -> takes force into account

  // static push
  // Read -> (inaccessible, not a JSON, not a Schema) -- filter out a file entirely, but do not short-circuit the process
  // Read -> (incompatible path) -- short-circuit the process
  // Read -> load everything into memory
  // Lint -> on very permissive set of checks (only errors)
  // Write -> short circuits at any point
  // Write -> takes force into account

  // static s3cp
  // Read -> (inaccessible) -- short-circuit the process
  // Read -> does not load into memory (it can be big)
  // Read -> does not even parse them



  def run(args: List[String]): IO[ExitCode] = {
    val result: Result = Command.parse(args) match {
      case Right(command: Command.Lint) =>
        Lint.process(command)

      case Right(command: Command.StaticGenerate) =>
        Generate.process(command)

      case Right(command: Command.StaticPush) =>
        withClient { client => Push.process(command, client)}

      case Right(command: Command.StaticPull) =>
        withClient { client => Pull.process(command, client)}

      case Right(command: Command.StaticS3Cp) =>
        S3cp.process(command)

      case Right(command: Command.ServerKeygen) =>
        withClient { client => Keygen.process(command, client)}

      case Right(command: Command.StaticDeploy) =>
        withClient { client => Deploy.process(command, client)}

      case Right(command: Command.TableCheck) =>
        withClient { client => TableCheck.process(command, client)}

      case Right(command: Command.TableMigrate) =>
        TableMigrate.process(command)

      case Right(Command.VersionFlag) =>
        EitherT.fromEither[IO](List(generated.ProjectSettings.version).asRight)

      case Left(e) =>
        EitherT.fromEither[IO](Error.Message(e.toString).asLeft[List[String]].toEitherNel)
    }

    result.value.flatMap(processResult[String])
  }

  val httpClientResource: Resource[IO, Client[IO]] = EmberClientBuilder.default[IO].build

  def withClient(f: Client[IO] => Result): Result = EitherT(httpClientResource.use { client => f(FollowRedirect(20)(client)).value })

  def processResult[A: Show](either: EitherNel[Error, List[A]]): IO[ExitCode] =
    either.fold(
      errors => errors.traverse_(e => IO(System.err.println(e.show))) *> IO.pure(ExitCode.Error),
      messages => messages.traverse_(e => IO(System.out.println(e.show))) *> IO.pure(ExitCode.Success)
    )
}
