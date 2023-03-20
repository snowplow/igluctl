package com.snowplowanalytics.iglu.ctl.commands

import com.snowplowanalytics.iglu.ctl.Command
import org.specs2.mutable.Specification

import java.nio.file.{Path, Paths}

class VerifyParquetSpec extends Specification {
  
  "Verify parquet command" should {
    "return message about breaking changes " in {
      val result = VerifyParquet.process(
        Command.VerifyParquet(
          input = testResourcePath("schemas/verify-parquet/breaking"),
        )
      )

      result.value.unsafeRunSync() must beRight(List("Breaking change between 'com.test/test/jsonschema/1-0-0' and 'com.test/test/jsonschema/1-0-1'"))
    }
    "return message no breaking changes detected" in {
      val result = VerifyParquet.process(
        Command.VerifyParquet(
          input = testResourcePath("schemas/verify-parquet/non-breaking"),
        )
      )

      result.value.unsafeRunSync() must beRight(List("No breaking changes detected"))
    }
  }

  private def testResourcePath(path: String): Path = Paths.get(getClass.getClassLoader.getResource(path).toURI)
}
