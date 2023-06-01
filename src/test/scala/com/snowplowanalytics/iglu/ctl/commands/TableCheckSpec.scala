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


import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.iglu.ctl.Storage.{Column, Comment}
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.Result
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.Result.TableIssue
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.Result.TableIssue.{AdditionalColumnInStorage, ColumnMismatch, CommentProblem, MissingColumnInStorage}
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelEntry
import org.specs2.Specification

class TableCheckSpec extends Specification { def is =s2"""
  checkColumns function in Table Check command
    returns error when there are more expected columns than existing columns $e1
    returns error when there are more existing columns than expected columns $e2
    returns error when number columns are equal but their names are different $e3
    returns error when number columns are equal but their types are different $e4
    returns error when number columns are equal but their nullablity is different $e5
    returns error when existing columns list is empty $e6
    returns error when comment is missing $e7
    returns error when comment is invalid $e8
    returns success when existing and expected column names are same but with different order $e9
    returns success when existing and expected columns are matched with each other $e10
  """

  val schemaKey = SchemaKey("com.vendor", "example", "jsonschema", SchemaVer.Full(1, 0, 0))

  def e1 = {
    assertUnmatched(
      existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0")),
      existingColumns = List(int("a"), int("b"), int("c")),
      expectedColumns = List(int("a"), int("b"), int("c"), int("d")),
      result = List(MissingColumnInStorage(int("d")))
    )
  }

  def e2 = {
    assertUnmatched(
      existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0")),
      existingColumns = List(int("a"), int("b"), int("c"), int("d")),
      expectedColumns = List(int("a"), int("b"), int("c")),
      result = List(AdditionalColumnInStorage(int("d")))
    )
  }

  def e3 = {
    assertUnmatched(
      existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0")),
      existingColumns = List(int("a"), int("b"), int("c"), int("f"), int("g")),
      expectedColumns = List(int("a"), int("b"), int("c"), int("d"), int("e")),
      result = List(
        AdditionalColumnInStorage(int("f")),
        AdditionalColumnInStorage(int("g")),
        MissingColumnInStorage(int("e")),
        MissingColumnInStorage(int("d"))
      )
    )
  }
  

  def e4 = {
    assertUnmatched(
      existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0")),
      existingColumns = List(int("a"), int("b"), boolean("c"), int("d"), int("e")),
      expectedColumns = List(int("a"), int("b"), int("c"), int("d"), int("e")),
      result = List(ColumnMismatch(expected = int("c"), existing = boolean("c")))
    )
  }

  def e5 = {
    assertUnmatched(
      existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0")),
      existingColumns = List(int("a"), int("b"), int("c", isNullable = false), int("d"), int("e")),
      expectedColumns = List(int("a"), int("b"), int("c", isNullable = true), int("d"), int("e")),
      result = List(ColumnMismatch(expected = int("c", isNullable = true), existing = int("c", isNullable = false)))
    )
  }

  def e6 = {
    val existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0"))
    val existingColumns = List.empty
    val expectedColumns = List(int("a"), int("b"), int("c"), int("d"), int("e"))

    TableCheck.verifyExistingStorage(schemaKey, existingComment, existingColumns, expectedColumns, schemaKey) must beEqualTo(
      Result.TableNotDeployed(schemaKey)
    )
  }

  def e7 = {
    assertUnmatched(
      existingComment = None,
      existingColumns = List(int("a"), int("b"), int("c"), int("d"), int("e")),
      expectedColumns = List(int("a"), int("b"), int("c"), int("d"), int("e")),
      result = List(CommentProblem("Table comment is missing"))
    )
  }
  
  def e8 = {
    assertUnmatched(
      existingComment = Some(Comment("iglu:com.vendor_another/example/jsonschema/1-0-0")),
      existingColumns = List(int("a"), int("b"), int("c"), int("d"), int("e")),
      expectedColumns = List(int("a"), int("b"), int("c"), int("d"), int("e")),
      result = List(CommentProblem("SchemaKey found in table comment [iglu:com.vendor_another/example/jsonschema/1-0-0] does not match expected model [iglu:com.vendor/example/jsonschema/1-0-0]"))
    )
  }

  def e9 = {
    val existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0"))
    val existingColumns = List(int("a"), int("b"), int("e"), int("c"), int("d"))
    val expectedColumns = List(int("a"), int("b"), int("c"), int("d"), int("e"))

    TableCheck.verifyExistingStorage(schemaKey, existingComment, existingColumns, expectedColumns, schemaKey) must beEqualTo(
      Result.TableMatched(schemaKey)
    )
  }

  def e10 = {
    val existingComment = Some(Comment("iglu:com.vendor/example/jsonschema/1-0-0"))
    val existingColumns = List(int("a"), int("b"), int("c"), int("d"), int("e"))
    val expectedColumns = List(int("a"), int("b"), int("c"), int("d"), int("e"))

    TableCheck.verifyExistingStorage(schemaKey, existingComment, existingColumns, expectedColumns, schemaKey) must beEqualTo(
      Result.TableMatched(schemaKey)
    )
  }

  private def assertUnmatched(
    existingComment: Option[Comment],
    existingColumns: List[Column],
    expectedColumns: List[Column],
    result: List[TableIssue]
  ) = {
    TableCheck.verifyExistingStorage(schemaKey, existingComment, existingColumns, expectedColumns, schemaKey) must beEqualTo(
      Result.TableUnmatched(schemaKey, Result.TableIssues(NonEmptyList.fromListUnsafe(result), expectedColumns, existingColumns))
    ) 
  }
  
  private def int(name: String, isNullable: Boolean = true): Column = Column(name, `type` = ShredModelEntry.ColumnType.RedshiftInteger, isNullable)
  private def boolean(name: String): Column = Column(name, `type` = ShredModelEntry.ColumnType.RedshiftBoolean, isNullable = true)
}
