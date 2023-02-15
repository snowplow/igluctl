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
package com.snowplowanalytics.iglu.ctl.commands

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.SchemaVer.Full
import com.snowplowanalytics.iglu.ctl.Command
import com.snowplowanalytics.iglu.ctl.Common.Error.Message
import org.specs2.matcher.EventuallyMatchers
import org.specs2.mutable.Specification

import java.nio.file.{Path, Paths}

class LintSpec extends Specification with EventuallyMatchers {

  "process" >> {
    "lint a directory of valid schemas" >> {
      val result = Lint.process(
        Command.Lint(
          input = testResourcePath("schemas/valid-schemas"),
          skipChecks = Nil,
          skipSchemas = Nil
        )
      )
      val expected = Set("OK: com.acme/signup_click/jsonschema/1-0-0", "OK: com.maxmind/anonymous_ip/jsonschema/1-0-0", "TOTAL: 2 valid schemas", "TOTAL: 0 schemas didn't pass validation")
      eventually(result.value.unsafeRunSync().map(_.toSet) must beRight(===(expected)))
    }

    "lint a schema with warnings" >> {
      val result = Lint.process(
        Command.Lint(
          input = testResourcePath("schemas"),
          skipChecks = Nil,
          skipSchemas = Nil
        )
      )
      eventually(result.value.unsafeRunSync() must beLeft())
    }

    "skip specific schemas in the linting" >> {
      val result = Lint.process(
        Command.Lint(
          input = testResourcePath("schemas"),
          skipChecks = Nil,
          skipSchemas = List(SchemaKey("com.acme", "signup_click","jsonschema", Full(1,0,1)))
        )
      )
      val expected = Set("OK: com.acme/signup_click/jsonschema/1-0-0", "OK: com.maxmind/anonymous_ip/jsonschema/1-0-0", "TOTAL: 2 valid schemas", "TOTAL: 0 schemas didn't pass validation")
      eventually(result.value.unsafeRunSync().map(_.toSet) must beRight(===(expected)))
    }

    "return an error if you skip all the schemas" >> {
      val result = Lint.process(
        Command.Lint(
          input = testResourcePath("schemas/warn-schemas"),
          skipChecks = Nil,
          skipSchemas = List(SchemaKey("com.acme", "signup_click","jsonschema", Full(1,0,1)))
        )
      )
      val errorMessages = result.value.unsafeRunSync().swap.toOption.get.toList
      eventually(errorMessages must contain(Message("All schemas provided were also skipped")))
    }
  }

  private def testResourcePath(path: String): Path = Paths.get(getClass.getClassLoader.getResource(path).toURI)

}
