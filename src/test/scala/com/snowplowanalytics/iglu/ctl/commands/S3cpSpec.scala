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
package com.snowplowanalytics.iglu.ctl.commands

import java.nio.file.Path

import org.specs2.mutable.Specification

class S3cpSpec extends Specification {

  "fileToS3Path" >> {
    "should resolve file names with no custom prefix" >> {
      val path = Path.of("com.acme", "myschema", "jsonschema", "1-0-0")
      val result = S3cp.fileToS3Path(path, None)

      result must_== "schemas/com.acme/myschema/jsonschema/1-0-0"
    }

    "should resolve top level path with no custom prefix" >> {
      val path = Path.of("")
      val result = S3cp.fileToS3Path(path, None)

      result must_== "schemas"
    }

    "should resolve file names with a custom prefix" >> {
      val path = Path.of("com.acme", "myschema", "jsonschema", "1-0-0")
      List("myprefix", "/myprefix", "myprefix/").map { prefix =>
        val result = S3cp.fileToS3Path(path, Some(prefix))

        result must_== "myprefix/schemas/com.acme/myschema/jsonschema/1-0-0"
      }
    }
  }
}
