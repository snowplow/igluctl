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
package com.snowplowanalytics.iglu.ctl

import cats.data._
import cats.effect._
import cats.implicits._
import com.snowplowanalytics.iglu.ctl.Storage.{columnsQuery, commonColumns}
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelEntry
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelEntry.ColumnType
import doobie.Read
import doobie.implicits._
import doobie.util.fragment
import doobie.util.transactor.Transactor

case class Storage[F[_]](xa: Transactor[F]) {

  def getColumns(tableName: String, tableSchema: String)(implicit F: Bracket[F, Throwable]): F[List[Storage.Column]] = {
    columnsQuery(tableName, tableSchema)
      .query[Storage.Column]
      .to[List]
      .transact(xa)
      .map(removeCommonColumns)
  }

  def getComment(tableName: String, tableSchema: String)(implicit F: Bracket[F, Throwable]): F[Option[Storage.Comment]] =
    fr"""
      SELECT obj_description(pg_class.oid) AS comment
      FROM pg_class
      INNER JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
      WHERE pg_class.relname = $tableName
      AND pg_namespace.nspname = $tableSchema
    """
      .query[Storage.CommentRow].option.transact(xa)
      .map(_.flatMap(_.objDescription.map(Storage.Comment)))

  private def removeCommonColumns(queriedColumns: List[Storage.Column]): List[Storage.Column] = {
    queriedColumns.filterNot { column =>
      commonColumns.contains(column.name)
    }
  }

}

object Storage {

  private val commonColumns: List[String] = List(
    "schema_vendor",
    "schema_name",
    "schema_format",
    "schema_version",
    "root_id",
    "root_tstamp",
    "ref_root",
    "ref_tree",
    "ref_parent"
  )
        
  private def columnsQuery(tableName: String, schema: String): fragment.Fragment =
    sql"""SELECT
        column_name,
        data_type,
        is_nullable,
        character_maximum_length,
        numeric_precision,
        numeric_scale 
        FROM information_schema.columns
        WHERE table_name = $tableName
        AND table_schema = $schema  
        ORDER BY ordinal_position""" 


  private implicit val readColumn: Read[Column] =
    Read[(String, String, Boolean, Option[Int], Option[Int], Option[Int])]
      .map {
        case (name, dataType, isNullable, maxLength, precision, scale) =>
          val ddlType = dataType match {
            case "timestamp without time zone" => ColumnType.RedshiftTimestamp
            case "date" => ColumnType.RedshiftDate
            case "integer" => ColumnType.RedshiftInteger
            case "smallint" => ColumnType.RedshiftSmallInt
            case "bigint" => ColumnType.RedshiftBigInt
            case "double precision" => ColumnType.RedshiftDouble
            case "numeric" => ColumnType.RedshiftDecimal(precision, scale)
            case "boolean" => ColumnType.RedshiftBoolean
            case "character varying" => ColumnType.RedshiftVarchar(maxLength.getOrElse(256))
            case "character" => ColumnType.RedshiftChar(maxLength.getOrElse(1))
            case other => throw new RuntimeException(s"Unknown storage data type - $other")
          }

          Column(name, ddlType, isNullable)
      }

  private case class CommentRow(objDescription: Option[String])
  
  case class Column(name: String, `type`: ShredModelEntry.ColumnType, isNullable: Boolean)
  case class Comment(value: String)

  case class DbConfig(host: String,
                      port: Int,
                      dbname: String,
                      username: String,
                      password: String)

  def initialize[F[_]: Effect: ContextShift](storageConfig: DbConfig,
                                             blocker: Blocker): Storage[F] = storageConfig match {
    case DbConfig(host, port, dbname, username, password) =>
      val url = s"jdbc:postgresql://$host:$port/$dbname?loggerLevel=OFF"
      val xa = Transactor.fromDriverManager[F]("org.postgresql.Driver", url, username, password, blocker)
      Storage(xa)
  }

  /**
    * Try to fetch missing db config fields from environment variables
    */
  def resolveDbConfig(commandDbConfig: Command.DbConfig): Failing[Storage.DbConfig] = {
    val res = for {
      host     <- resolveOptArgument("PGHOST", commandDbConfig.host)
      dbName   <- resolveOptArgument("PGDATABASE", commandDbConfig.dbname)
      username <- resolveOptArgument("PGUSER", commandDbConfig.username)
      password <- resolveOptArgument("PGPASSWORD", commandDbConfig.password)
    } yield (host, commandDbConfig.port.validNel, dbName, username, password).mapN(Storage.DbConfig.apply)
    EitherT(
      res.map { validated =>
        validated.toEither.leftMap { errors =>
          Common.Error.Message(errors.mkString_("\n"))
        }
      }
    )
  }

  /**
    * In case of given optional argument is None, try to fetch given
    * environment variable. If env variable exists, return it. Otherwise,
    * return error.
    */
  private def resolveOptArgument(envVariable: String, optionalArg: Option[String]): IO[ValidatedNel[String, String]] =
    IO.delay {
      optionalArg match {
        case None => sys.env.get(envVariable).toValidNel[String](s"$envVariable is not set")
        case Some(arg) => arg.validNel[String]
      }
    }
}
