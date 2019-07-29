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

import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.Storage.Column

import com.snowplowanalytics.iglu.schemaddl.redshift.{RedshiftInteger, Column => DDLColumn}

import org.specs2.Specification

class TableCheckSpec extends Specification { def is =s2"""
  checkColumns function in Table Check command
    returns error when there are more expected columns than existing columns $e1
    returns error when there are more existing columns than expected columns $e2
    returns error when number columns are equal but their names are different $e3
    returns success when existing and expected columns are matched with each other $e4
  """

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
    TableCheck.checkColumns(existingColumns, expectedColumns) must beLeft(
      Error.Message(
        """
          |Existing and expected columns are different:
          |  existing: a,b,c
          |  expected: a,b,c,d""".stripMargin
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
    TableCheck.checkColumns(existingColumns, expectedColumns) must beLeft(
      Error.Message(
        """
          |Existing and expected columns are different:
          |  existing: a,b,c,d
          |  expected: a,b,c""".stripMargin
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
    TableCheck.checkColumns(existingColumns, expectedColumns) must beLeft(
      Error.Message(
        """
          |Existing and expected columns are different:
          |  existing: a,b,c,f,g
          |  expected: a,b,c,d,e""".stripMargin
      )
    )
  }

  def e4 = {
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
    TableCheck.checkColumns(existingColumns, expectedColumns) must beRight
  }
}
