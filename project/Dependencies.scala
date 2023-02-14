/*
 * Copyright (c) 2016-2022 Snowplow Analytics Ltd. All rights reserved.
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
import sbt._

object Dependencies {

  object V {
    // Java
    val awsJava          = "2.17.213"
    val jackson          = "2.12.7"
    // Scala
    val circe            = "0.14.3"
    val circeConfig      = "0.10.0"
    val catsEffect       = "2.5.3"
    val schemaddl        = "0.18.0-M17"
    val igluClient       = "1.3.0"
    val decline          = "1.4.0"
    val http4s           = "0.21.33"
    val fs2              = "2.5.9"
    val doobie           = "0.13.4"
    val logback          = "1.2.3"
    // Scala (test only)
    val specs2           = "4.12.3"
    val scalaCheck       = "1.15.4"
  }

  object Libraries {
    // Java
    val awsJava          = "software.amazon.awssdk"     %  "s3"                        % V.awsJava
    val jacksonDatabind  = "com.fasterxml.jackson.core" % "jackson-databind"           % V.jackson // override transitive version to address security vulnerabilities

    // Scala
    val circeParser      = "io.circe"                   %% "circe-jawn"                % V.circe
    val circeConfig      = "io.circe"                   %% "circe-config"              % V.circeConfig
    val catsEffect       = "org.typelevel"              %% "cats-effect"               % V.catsEffect
    val igluClient       = "com.snowplowanalytics"      %% "iglu-scala-client"         % V.igluClient
    val schemaddl        = "com.snowplowanalytics"      %% "schema-ddl"                % V.schemaddl
    val decline          = "com.monovore"               %% "decline"                   % V.decline
    val http4sEmberCli   = "org.http4s"                 %% "http4s-ember-client"       % V.http4s
    val http4sCirce      = "org.http4s"                 %% "http4s-circe"              % V.http4s
    val http4sDsl        = "org.http4s"                 %% "http4s-dsl"                % V.http4s
    val fs2              = "co.fs2"                     %% "fs2-core"                  % V.fs2
    val fs2Io            = "co.fs2"                     %% "fs2-io"                    % V.fs2
    val doobieCore       = "org.tpolecat"               %% "doobie-core"               % V.doobie
    val logback          = "ch.qos.logback"             % "logback-classic"            % V.logback % Runtime
    // Scala (test only)
    val specs2           = "org.specs2"                 %% "specs2-core"               % V.specs2         % "test"
    val scalaCheck       = "org.scalacheck"             %% "scalacheck"                % V.scalaCheck     % "test"
    val circeLiteral     = "io.circe"                   %% "circe-literal"             % V.circe          % "test"
  }
}
