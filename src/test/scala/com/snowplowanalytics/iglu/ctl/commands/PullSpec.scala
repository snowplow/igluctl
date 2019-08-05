/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.ctl.Common
import org.specs2.Specification
import scalaj.http.HttpResponse

class PullSpec extends Specification { def is = s2"""
  parseResponse function in Pull command
    return error if response status is not 200 $e1
    return error if there is an invalid schema in response body $e2
    return error if response body is invalid json $e3
    successfully parses response with valid schema $e4
  """

  def e1 = {
    val expected = Common.Error.Message("Unexpected status code 400. Response body: body.")
    val response = HttpResponse("body", 400, Map("header" -> IndexedSeq("header")))
    val res = Pull.parseResponse(response)
    res must beLeft(expected)
  }

  def e2 = {
    val responseBodyWithInvalidSchema =
      """
        |[
        |  {
        |    "type": "object",
        |    "properties": {
        |      "name": {
        |        "type": "string",
        |        "maxLength": 4096
        |      },
        |      "value": {
        |        "type": [
        |          "string",
        |          "null"
        |        ],
        |        "maxLength": 4096
        |      }
        |    },
        |    "required": [
        |      "name",
        |      "value"
        |    ],
        |    "additionalProperties": false
        |  }
        |]""".stripMargin

    val expected = Common.Error.Message("Error while parsing response: InvalidSchema")
    val response = HttpResponse(responseBodyWithInvalidSchema, 200, Map("header" -> IndexedSeq("header")))
    val res = Pull.parseResponse(response)

    res must beLeft(expected)
  }

  def e3 = {
    val invalidJson =
      """
        |[
        |    "type": "object",
        |    "properties": {
        |      "name": {
        |        "type": "string",
        |        "maxLength": 4096
        |      },
        |      "value": {
        |        "type": [
        |          "string",
        |          "null"
        |        ],
        |        "maxLength": 4096
        |      }
        |    },
        |    "required": [
        |      "name",
        |      "value"
        |    ],
        |    "additionalProperties": false
        |  }
        |]""".stripMargin

    val response = HttpResponse(invalidJson, 200, Map("header" -> IndexedSeq("header")))
    val res = Pull.parseResponse(response)

    res must beLeft
  }

  def e4 = {
    val validResponseBody =
      """
        |[
        |  {
        |	  "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        |	  "description": "Schema for a single HTTP cookie, as defined in RFC 6265",
        |	  "self": {
        |		  "vendor": "org.ietf",
        |		  "name": "http_cookie",
        |		  "format": "jsonschema",
        |		  "version": "1-0-0"
        |	  },
        |
        |	  "type": "object",
        |	  "properties": {
        |		  "name": {
        |			  "type": "string",
        |			  "maxLength" : 4096
        |		  },
        |		  "value": {
        |			  "type": ["string", "null"],
        |			  "maxLength" : 4096
        |		  }
        |	  },
        |	  "required": ["name", "value"],
        |	  "additionalProperties": false
        |  }
        |]""".stripMargin

    val response = HttpResponse(validResponseBody, 200, Map("header" -> IndexedSeq("header")))
    val res = Pull.parseResponse(response)

    res must beRight
  }
}
