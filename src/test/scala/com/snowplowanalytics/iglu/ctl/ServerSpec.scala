package com.snowplowanalytics.iglu.ctl

import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import org.specs2.matcher.EventuallyMatchers
import org.specs2.mutable.Specification
import io.circe.literal._
import com.snowplowanalytics.iglu.core.circe.implicits._
import org.http4s.{Header, Method}
import org.http4s.implicits._

import java.util.UUID

class ServerSpec extends Specification with EventuallyMatchers {

  "buildPushRequest" >> {
    "build a request without swallowing a path included on the iglu server root url" >> {
      val schemaJson =
        json"""
          {
           "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
           "self": {
             "vendor": "com.acme",
             "name": "geo_location",
             "format": "jsonschema",
             "version": "1-0-0"
           },
           "description": "geo location",
           "properties": {
             "country": {
               "type": "string",
               "maxLength": 255,
               "description": "Mandatory field to describe the country"
             }
           },
           "additionalProperties": true,
           "type": "object",
           "required": [
             "country"
           ]
          }
          """
      val schema = SelfDescribingSchema.parse(schemaJson).getOrElse(throw new RuntimeException("Invalid self-describing JSON schema"))
      val apikey = "2ff29738-fb29-4b8d-848b-fab1d3699748"
      val req = Server.buildPushRequest(Server.HttpUrl(uri = uri"http://localhost:2000/iglu-server"), true, schema, UUID.fromString(apikey))

      req.uri mustEqual uri"http://localhost:2000/iglu-server/api/schemas/com.acme/geo_location/jsonschema/1-0-0?isPublic=true"
      req.method must be (Method.PUT)
      req.headers.toList must contain(Header("apikey", apikey))
    }
  }
}