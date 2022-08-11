/**
  * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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

import Dependencies._
import BuildSettings._

lazy val root = project.in(file("."))
  .settings(
    name                  :=  "igluctl",
    organization          :=  "com.snowplowanalytics",
    version               :=  "0.10.1",
    description           :=  "Iglu Command Line Interface",
    scalaVersion          :=  "2.12.14"
  )
  .settings(buildSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Libraries.awsJava,
      Libraries.jacksonDatabind,
      // Scala
      Libraries.circeParser,
      Libraries.circeConfig,
      Libraries.catsEffect,
      Libraries.schemaddl,
      Libraries.igluClient,
      Libraries.decline,
      Libraries.http4sEmberCli,
      Libraries.http4sCirce,
      Libraries.http4sDsl,
      Libraries.fs2,
      Libraries.fs2Io,
      Libraries.doobieCore,
      Libraries.logback,
      // Scala (test only)
      Libraries.specs2,
      Libraries.scalaCheck,
      Libraries.circeLiteral
    )
  )
