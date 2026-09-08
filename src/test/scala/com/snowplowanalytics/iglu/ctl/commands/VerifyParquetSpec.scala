package com.snowplowanalytics.iglu.ctl.commands

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.ctl.Command
import com.snowplowanalytics.iglu.ctl.SpecHelpers._
import io.circe.Json
import io.circe.literal._
import org.specs2.mutable.Specification

import java.nio.file.{Path, Paths}
import cats.effect.unsafe.implicits.global

class VerifyParquetSpec extends Specification {
  
  "Verify parquet command" should {
    "return message about breaking changes " in {
      val result = VerifyParquet.process(
        Command.VerifyParquet(
          input = testResourcePath("verify-parquet/breaking"),
        )
      )

      result.value.unsafeRunSync() must beRight(List("Breaking change introduced by 'com.test/test/jsonschema/1-0-1'. Changes: Incompatible type change String to Double at /id"))
    }
    "return message no breaking changes detected" in {
      val result = VerifyParquet.process(
        Command.VerifyParquet(
          input = testResourcePath("verify-parquet/non-breaking"),
        )
      )

      result.value.unsafeRunSync() must beRight(List("No breaking changes detected"))
    }
    "detect the same breaking changes whatever order the schemas arrive in" in {
      def testSchema(version: String, idType: Json) = json"""
        {
          "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "description": "Test schema",
          "self": {
            "vendor": "com.test",
            "name": "test",
            "format": "jsonschema",
            "version": $version
          },
          "type": "object",
          "properties": { "id": { "type": $idType } },
          "additionalProperties": false
        }""".schema

      val initial = testSchema("1-0-0", json""""string"""")
      val second = testSchema("1-0-1", json""""number"""")

      val expected = List("Breaking change introduced by 'com.test/test/jsonschema/1-0-1'. Changes: Incompatible type change String to Double at /id")

      // A string sort of file paths reverses these once a family reaches ten additions, so the
      // comparison must not depend on the order the schemas are handed over in
      VerifyParquet.verify(NonEmptyList.of(initial, second)) must beEqualTo(expected)
      VerifyParquet.verify(NonEmptyList.of(second, initial)) must beEqualTo(expected)
    }
    "still report breaking changes when a schema with no fields is in the input" in {
      val result = VerifyParquet.process(
        Command.VerifyParquet(
          input = testResourcePath("verify-parquet/breaking-with-fieldless"),
        )
      )

      result.value.unsafeRunSync() must beRight(List("Breaking change introduced by 'com.test/test/jsonschema/1-0-1'. Changes: Incompatible type change String to Double at /id"))
    }
    "return message no breaking changes detected when every schema has no fields" in {
      val result = VerifyParquet.process(
        Command.VerifyParquet(
          input = testResourcePath("verify-parquet/only-fieldless"),
        )
      )

      result.value.unsafeRunSync() must beRight(List("No breaking changes detected"))
    }
  }

  private def testResourcePath(path: String): Path = Paths.get(getClass.getClassLoader.getResource(path).toURI)
}
