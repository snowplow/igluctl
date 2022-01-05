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

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.iglu.schemaddl.redshift.{RedshiftInteger, Column => DDLColumn}

import com.snowplowanalytics.iglu.ctl.Common
import com.snowplowanalytics.iglu.ctl.Storage.Column
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.Result._

import org.specs2.Specification

class TableMigrateSpec extends Specification { def is = s2"""
  Table Migrate command specification
    return error if table is not deployed $e1
    return error if table is matching to expected $e2
    return error if table is not matched and number of expected columns are more than existing ones $e3
    return error if table is not matched and number of existing columns are more than expected ones $e4
    return error if table is not matched and number columns are equal but their names are different $e5
    return migrations if table is not matched and only ordering of expected and existing columns are different $e6
  """

  val schemaKey = SchemaKey("com.acme", "example", "jsonschema", SchemaVer.Full(1, 0, 0))

  def e1 = {
    val notDeployed = TableNotDeployed(schemaKey)
    val res = TableMigrate.getResult(notDeployed, "", "", "")
    val expected = Common.Error.Message("Table not deployed")
    res must beLeft(expected)
  }

  def e2 = {
    val matched = TableMatched(schemaKey)
    val res = TableMigrate.getResult(matched, "", "", "")
    val expected = Common.Error.Message("Table matched")
    res must beLeft(expected)
  }

  def e3 = {
    val existingColumns = List(
      Column("a"), Column("b"), Column("c")
    )
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger),
      DDLColumn("d", RedshiftInteger)
    )
    val notMatched = TableUnmatched(schemaKey, existingColumns, expectedColumns)
    val res = TableMigrate.getResult(notMatched, "", "", "")
    val expected = Common.Error.Message("Columns are different, can not create migration statements")
    res must beLeft(expected)
  }

  def e4 = {
    val existingColumns = List(
      Column("a"), Column("b"), Column("c"), Column("d")
    )
    val expectedColumns = List(
      DDLColumn("a", RedshiftInteger),
      DDLColumn("b", RedshiftInteger),
      DDLColumn("c", RedshiftInteger)
    )
    val notMatched = TableUnmatched(schemaKey, existingColumns, expectedColumns)
    val res = TableMigrate.getResult(notMatched, "", "", "")
    val expected = Common.Error.Message("Columns are different, can not create migration statements")
    res must beLeft(expected)
  }

  def e5 = {
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
    val notMatched = TableUnmatched(schemaKey, existingColumns, expectedColumns)
    val res = TableMigrate.getResult(notMatched, "", "", "")
    val expected = Common.Error.Message("Columns are different, can not create migration statements")
    res must beLeft(expected)
  }

  def e6 = {
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
    val notMatched = TableUnmatched(schemaKey, existingColumns, expectedColumns)
    val res = TableMigrate.getResult(notMatched, "s3_path", "aws_role", "aws_region")
    val expected =
      """UNLOAD ('SELECT a,b,c,d,e FROM com_acme_example_1')
        |  TO 's3_path'
        |  iam_role 'aws_role'
        |  DELIMITER '\t';
        |ALTER TABLE com_acme_example_1 RENAME TO com_acme_example_1_legacy;
        |COPY com_acme_example_1  FROM 's3_path'
        |  CREDENTIALS 'aws_iam_role=aws_role'
        |  REGION AS 'aws_region'
        |  DELIMITER '\t'
        |  MAXERROR 0 TRUNCATECOLUMNS TIMEFORMAT 'auto'
        |  ACCEPTINVCHARS;""".stripMargin
    res must beRight(expected)
  }
}
