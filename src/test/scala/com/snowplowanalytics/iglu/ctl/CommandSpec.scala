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

import cats.implicits._

// java
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.SchemaVer.Full

import java.nio.file.Paths
import java.util.UUID

// decline
import com.monovore.decline.Help

// Schema DDL
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter.{ unknownFormats, rootObject}

// specs2
import org.specs2.Specification

// File(".") used everywhere because directory must be available for read
class CommandSpec extends Specification { def is = s2"""
  Command.parse specification
    extracts lint command class $e1
    extracts static push command class $e2
    extracts static s3cp command class $e3
    extracts lint command class (--skip-checks) $e4
    fails to extract lint with unskippable checks specified $e5
    extracts static pull command class when apikey is not given $e6
    extracts static pull command class when apikey is given $e7
    extracts lint command class (--skip-schemas) $e8
    extracts verify parquet command $e9
    extracts verify redshift command without apikey $e10
    extracts verify redshift command with apikey $e11
    extracts verify redshift command with long verbose flag $e12
    extracts verify redshift command with short verbose flag $e13
  """

  def e1 = {
    val lint = Command.parse("lint .".split(" ").toList)

    lint must beRight(Command.Lint(Paths.get("."), List.empty, List.empty))
  }

  def e2 = {
    val staticPush = Command.parse("static push .. http://54.165.217.26:8081/ 1af851ab-ef1b-4109-a8e2-720ac706334c --public".split(" ").toList)

    val url = Server.HttpUrl.parse("http://54.165.217.26:8081/").getOrElse(throw new RuntimeException("Invalid URI"))
    staticPush must beRight(Command.StaticPush(Paths.get(".."), url, UUID.fromString("1af851ab-ef1b-4109-a8e2-720ac706334c"), true))
  }

  def e3 = {
    val staticS3cp = Command
      .parse("static s3cp .. anton-enrichment-test --s3path schemas --region us-east-1".split(" ").toList)

    staticS3cp must beRight(Command.StaticS3Cp(Paths.get(".."), "anton-enrichment-test", Some("schemas"), None, None, None, Some("us-east-1"), false))
  }

  def e4 = {
    val lint = Command.parse("lint . --skip-checks unknownFormats,rootObject".split(" ").toList)

    val skippedChecks = List(rootObject, unknownFormats)

    lint must beRight(Command.Lint(Paths.get("."), skippedChecks, List.empty))
  }

  def e5 = {
    val lint = Command.parse("lint . --skip-checks requiredPropertiesExist".split(" ").toList)

    lint must beLeft.like {
      case Help(errors, _, _, _) => errors must beEqualTo(List("Configuration is invalid: non-skippable linters [requiredPropertiesExist]"))
      case _ => ko("Invalid error message")
    }
  }

  def e6 = {
    val staticPull = Command.parse("static pull .. http://54.165.217.26:8081/ 1af851ab-ef1b-4109-a8e2-720ac706334c".split(" ").toList)
    val url = Server.HttpUrl.parse("http://54.165.217.26:8081/").getOrElse(throw new RuntimeException("Invalid URI"))

    staticPull must beRight(Command.StaticPull(Paths.get(".."), url, Some(UUID.fromString("1af851ab-ef1b-4109-a8e2-720ac706334c"))))
  }

  def e7 = {
    val staticPull = Command.parse("static pull .. http://54.165.217.26:8081/".split(" ").toList)
    val url = Server.HttpUrl.parse("http://54.165.217.26:8081/").getOrElse(throw new RuntimeException("Invalid URI"))

    staticPull must beRight(Command.StaticPull(Paths.get(".."), url, None))
  }

  def e8 = {
    val lint = Command.parse("lint . --skip-schemas iglu:com.pagerduty/incident/jsonschema/1-0-0,iglu:com.mparticle.snowplow/app_event/jsonschema/1-0-0".split(" ").toList)

    val skippedSchemas = List(SchemaKey("com.pagerduty", "incident", "jsonschema", Full(1,0,0)), SchemaKey("com.mparticle.snowplow", "app_event", "jsonschema", Full(1,0,0)))

    lint must beRight(Command.Lint(Paths.get("."), List.empty, skippedSchemas))
  }

  def e9 = {
    val verify = Command.parse("verify parquet /tmp/snowplow".split(" ").toList)

    verify must beRight(Command.VerifyParquet(Paths.get("/tmp/snowplow")))
  }

  def e10 = {
    val verify = Command.parse("verify redshift --server http://localhost:8080".split(" ").toList)
    val url = Server.HttpUrl.parse("http://localhost:8080").getOrElse(throw new RuntimeException("Invalid URI"))

    verify must beRight(Command.VerifyRedshift(url, None, verbose = false))
  }

  def e11 = {
    val verify = Command.parse("verify redshift --server http://localhost:8080 --apikey e82d494f-ba11-4206-b78a-d2aaedeeab44".split(" ").toList)
    val url = Server.HttpUrl.parse("http://localhost:8080").getOrElse(throw new RuntimeException("Invalid URI"))

    verify must beRight(Command.VerifyRedshift(url, UUID.fromString("e82d494f-ba11-4206-b78a-d2aaedeeab44").some, verbose = false))
  }

  def e12 = {
    val verify = Command.parse("verify redshift --server http://localhost:8080 --apikey e82d494f-ba11-4206-b78a-d2aaedeeab44 --verbose".split(" ").toList)
    val url = Server.HttpUrl.parse("http://localhost:8080").getOrElse(throw new RuntimeException("Invalid URI"))

    verify must beRight(Command.VerifyRedshift(url, UUID.fromString("e82d494f-ba11-4206-b78a-d2aaedeeab44").some, verbose = true))
  }

  def e13 = {
    val verify = Command.parse("verify redshift --server http://localhost:8080 --apikey e82d494f-ba11-4206-b78a-d2aaedeeab44 -v".split(" ").toList)
    val url = Server.HttpUrl.parse("http://localhost:8080").getOrElse(throw new RuntimeException("Invalid URI"))

    verify must beRight(Command.VerifyRedshift(url, UUID.fromString("e82d494f-ba11-4206-b78a-d2aaedeeab44").some, verbose = true))
  }
}
