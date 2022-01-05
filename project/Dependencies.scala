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
    val awsJava          = "2.17.13"
    // Scala
    val circe            = "0.14.1"
    val circeConfig      = "0.8.0"
    val catsEffect       = "2.5.3"
    val schemaddl        = "0.14.1"
    val igluClient       = "1.1.1"
    val decline          = "1.4.0"
    val scalajHttp       = "2.4.2"
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
    // Scala
    val circeParser      = "io.circe"                   %% "circe-jawn"                % V.circe
    val circeConfig      = "io.circe"                   %% "circe-config"              % V.circeConfig
    val catsEffect       = "org.typelevel"              %% "cats-effect"               % V.catsEffect
    val igluClient       = "com.snowplowanalytics"      %% "iglu-scala-client"         % V.igluClient
    val schemaddl        = "com.snowplowanalytics"      %% "schema-ddl"                % V.schemaddl
    val decline          = "com.monovore"               %% "decline"                   % V.decline
    val scalajHttp       = "org.scalaj"                 %% "scalaj-http"               % V.scalajHttp
    val fs2              = "co.fs2"                     %% "fs2-core"                  % V.fs2
    val fs2Io            = "co.fs2"                     %% "fs2-io"                    % V.fs2
    val doobieCore       = "org.tpolecat"               %% "doobie-core"               % V.doobie
    val doobiePostgres   = "org.tpolecat"               %% "doobie-postgres"           % V.doobie
    val logback          = "ch.qos.logback"             % "logback-classic"            % V.logback % Runtime
    // Scala (test only)
    val specs2           = "org.specs2"                 %% "specs2-core"               % V.specs2         % "test"
    val scalaCheck       = "org.scalacheck"             %% "scalacheck"                % V.scalaCheck     % "test"
    val circeLiteral     = "io.circe"                   %% "circe-literal"             % V.circe          % "test"
  }
}
