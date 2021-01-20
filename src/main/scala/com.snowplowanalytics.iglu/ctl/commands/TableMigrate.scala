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

import cats.data._
import cats.effect._
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap}

import com.snowplowanalytics.iglu.schemaddl.{StringUtils => SchemaDDLStringUtils}
import com.snowplowanalytics.iglu.schemaddl.redshift.{Column => DDLColumn}

import com.snowplowanalytics.iglu.ctl.Command.{DbConfig, S3Path}
import com.snowplowanalytics.iglu.ctl.{Common, Result, Storage}
import com.snowplowanalytics.iglu.ctl.Storage.Column


object TableMigrate {

  val EventFieldSeparator = "\\t"

  /**
    * Primary method of table-migrate command. Checks whether table of the given schema
    * has expected structure which is determined with last and previous versions of
    * the given schema and if it has not expected structure, creates necessary migration
    * SQL statements
    */
  def process(resolver: Path,
              schema: SchemaKey,
              dbSchema: String,
              outputS3Path: S3Path,
              awsRole: String,
              awsRegion: String,
              dbConfig: DbConfig)(implicit cs: ContextShift[IO], t: Timer[IO]): Result =
    (for {
      resolvedDbConfig <- Storage.resolveDbConfig(dbConfig)
      res <- EitherT(Storage.initialize[IO](resolvedDbConfig).use { storage =>
        (for {
          tableCheckRes <- TableCheck.tableCheckSingle(resolver, schema, storage, dbSchema)
          res <- EitherT.fromEither[IO](getResult(tableCheckRes, outputS3Path, awsRole, awsRegion))
        } yield res).value
      })
    } yield res).leftMap(e => NonEmptyList.of(e)).map(e => List(e))

  private[ctl] def getResult(tableCheckResult: TableCheck.Result, outputS3Path: String, awsRole: String, awsRegion: String): Either[Common.Error, String] = {
    tableCheckResult match {
      case TableCheck.Result.TableNotDeployed(_) =>
        Common.Error.Message("Table not deployed").asLeft
      case TableCheck.Result.TableMatched(_) =>
        Common.Error.Message("Table matched").asLeft
      case TableCheck.Result.TableUnmatched(_, existing, expected) if !sameColumnsSet(existing, expected) =>
        Common.Error.Message("Columns are different, can not create migration statements").asLeft
      case TableCheck.Result.TableUnmatched(schemaKey, _, expected) =>
        createMigrationStatements(expected, outputS3Path, schemaKey, awsRole, awsRegion).asRight
      case TableCheck.Result.CommentProblem(_, message) =>
        Common.Error.Message(s"Table comment problem. ${message}").asLeft
    }
  }

  private def createMigrationStatements(expectedColumns: List[DDLColumn], outputS3Path: String, schemaKey: SchemaKey, awsRole: String, awsRegion: String): String = {
    val tableName = SchemaDDLStringUtils.getTableName(SchemaMap(schemaKey))
    val unloadStatement =
    s"""UNLOAD ('SELECT ${expectedColumns.map(_.columnName).mkString(",")} FROM $tableName')
      |  TO '$outputS3Path'
      |  iam_role '$awsRole'
      |  DELIMITER '$EventFieldSeparator';""".stripMargin
    val alterTableNameStatement = s"ALTER TABLE $tableName RENAME TO ${tableName}_legacy;"
    val copyStatement =
    s"""COPY $tableName  FROM '$outputS3Path'
      |  CREDENTIALS 'aws_iam_role=$awsRole'
      |  REGION AS '$awsRegion'
      |  DELIMITER '$EventFieldSeparator'
      |  MAXERROR 0 TRUNCATECOLUMNS TIMEFORMAT 'auto'
      |  ACCEPTINVCHARS;""".stripMargin
    s"""$unloadStatement
      |$alterTableNameStatement
      |$copyStatement""".stripMargin
  }

  private def sameColumnsSet(existingColumns: List[Column], expectedColumns: List[DDLColumn]): Boolean =
    existingColumns.map(_.columnName).toSet == expectedColumns.map(_.columnName).toSet
}
