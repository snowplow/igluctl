package com.snowplowanalytics.iglu.ctl.commands

import com.snowplowanalytics.iglu.ctl.Command
import org.specs2.mutable.Specification

import java.nio.file.{Path, Paths}

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
  }

  private def testResourcePath(path: String): Path = Paths.get(getClass.getClassLoader.getResource(path).toURI)
}
