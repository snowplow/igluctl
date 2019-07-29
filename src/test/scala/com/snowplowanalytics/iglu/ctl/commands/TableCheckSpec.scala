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


import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.iglu.schemaddl.redshift.{RedshiftInteger, Column => DDLColumn}

import com.snowplowanalytics.iglu.ctl.Storage.Column
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.Result

import org.specs2.Specification

class TableCheckSpec extends Specification { def is =s2"""
  checkColumns function in Table Check command
    returns error when there are more expected columns than existing columns $e1
    returns error when there are more existing columns than expected columns $e2
    returns error when number columns are equal but their names are different $e3
    returns error when existing and expected column names are same but with different order $e4
    returns error when existing columns list is empty $e5
    returns success when existing and expected columns are matched with each other $e6
  """

  val schemaKey = SchemaKey("com.vendor", "example", "jsonschema", SchemaVer.Full(1, 0, 0))

  def e1 = {
    val existingColumns = List(
      Column("a"), Column("b"), Column("c")
    )
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger),
      DDLColumn("d", RedshiftInteger)
    )
    TableCheck.checkColumns(schemaKey, existingColumns, expectedColumns) must beEqualTo(
      Result.TableUnmatched(
        schemaKey,
        existingColumns,
        expectedColumns
      )
    )
  }

  def e2 = {
    val existingColumns = List(
      Column("a"), Column("b"), Column("c"), Column("d")
    )
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger)
    )
    TableCheck.checkColumns(schemaKey, existingColumns, expectedColumns) must beEqualTo(
      Result.TableUnmatched(
        schemaKey,
        existingColumns,
        expectedColumns
      )
    )
  }

  def e3 = {
    val existingColumns = List(
      Column("a"), Column("b"), Column("c"), Column("f"), Column("g")
    )
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger),
      DDLColumn("d", RedshiftInteger),
      DDLColumn("e", RedshiftInteger)
    )
    TableCheck.checkColumns(schemaKey, existingColumns, expectedColumns) must beEqualTo(
      Result.TableUnmatched(
        schemaKey,
        existingColumns,
        expectedColumns
      )
    )
  }

  def e4 = {
    val existingColumns = List(
      Column("a"), Column("b"), Column("e"), Column("c"), Column("d")
    )
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger),
      DDLColumn("d", RedshiftInteger),
      DDLColumn("e", RedshiftInteger)
    )
    TableCheck.checkColumns(schemaKey, existingColumns, expectedColumns) must beEqualTo(
      Result.TableUnmatched(
        schemaKey,
        existingColumns,
        expectedColumns
      )
    )
  }

  def e5 = {
    val existingColumns = List.empty
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger),
      DDLColumn("d", RedshiftInteger),
      DDLColumn("e", RedshiftInteger)
    )
    TableCheck.checkColumns(schemaKey, existingColumns, expectedColumns) must beEqualTo(
      Result.TableNotDeployed(schemaKey)
    )
  }

  def e6 = {
    val existingColumns = List(
      Column("a"), Column("b"), Column("c"), Column("d"), Column("e")
    )
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger),
      DDLColumn("d", RedshiftInteger),
      DDLColumn("e", RedshiftInteger)
    )
    TableCheck.checkColumns(schemaKey, existingColumns, expectedColumns) must beEqualTo(
      Result.TableMatched(schemaKey)
    )
  }
}
