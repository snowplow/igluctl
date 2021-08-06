/*
 * Copyright (c) 2016-2021 Snowplow Analytics Ltd. All rights reserved.
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
    val jsonValidator    = "2.2.10"
    val awsJava          = "2.17.13"
    // JAXB APIs aren't resolved by default as of Java 9
    // https://docs.oracle.com/javase/9/migrate/#GUID-F640FA9D-FB66-4D85-AD2B-D931174C09A3
    val javaxXmlBind     = "2.3.0"
    val jaxbCore         = "2.3.0"
    val jaxbImpl         = "2.3.0"
    val activation       = "1.1.1"
    // Scala
    val circe            = "0.11.0"
    val catsEffect       = "2.5.3"
    val schemaddl        = "0.10.0"
    val igluClient       = "0.6.0"
    val decline          = "0.6.0"
    val scalajHttp       = "2.3.0"
    val fs2              = "1.0.2"
    val doobie           = "0.7.0"
    // Scala (test only)
    val specs2           = "4.0.1"
    val scalaCheck       = "1.13.5"
  }

  object Libraries {
    // Java
    val jsonValidator    = "com.github.java-json-tools" % "json-schema-validator"      % V.jsonValidator
    val awsJava          = "software.amazon.awssdk"     %  "s3"                        % V.awsJava
    // JAXB APIs
    val javaxXmlBind     = "javax.xml.bind"             %  "jaxb-api"                  % V.javaxXmlBind
    val jaxbCore         = "com.sun.xml.bind"           %  "jaxb-core"                 % V.jaxbCore
    val jaxbImpl         = "com.sun.xml.bind"           %  "jaxb-impl"                 % V.jaxbImpl
    val activation       = "javax.activation"           %  "activation"                % V.activation
    // Scala
    val circeParser      = "io.circe"                   %% "circe-jawn"                % V.circe
    val catsEffect       = "org.typelevel"              %% "cats-effect"               % V.catsEffect
    val igluClient       = "com.snowplowanalytics"      %% "iglu-scala-client"         % V.igluClient
    val schemaddl        = "com.snowplowanalytics"      %% "schema-ddl"                % V.schemaddl
    val decline          = "com.monovore"               %% "decline"                   % V.decline
    val scalajHttp       = "org.scalaj"                 %% "scalaj-http"               % V.scalajHttp
    val fs2              = "co.fs2"                     %% "fs2-core"                  % V.fs2
    val fs2Io            = "co.fs2"                     %% "fs2-io"                    % V.fs2
    val doobieCore       = "org.tpolecat"               %% "doobie-core"               % V.doobie
    val doobiePostgres   = "org.tpolecat"               %% "doobie-postgres"           % V.doobie
    // Scala (test only)
    val specs2           = "org.specs2"                 %% "specs2-core"               % V.specs2         % "test"
    val scalaCheck       = "org.scalacheck"             %% "scalacheck"                % V.scalaCheck     % "test"
    val circeLiteral     = "io.circe"                   %% "circe-literal"             % V.circe          % "test"
  }
}
