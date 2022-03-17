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

import cats.effect._
import cats.data._
import cats.implicits._

import doobie.implicits._
import doobie.util.transactor.Transactor

case class Storage[F[_]](xa: Transactor[F]) {

  /**
    * Fetch columns of the given database table
    */
  def getColumns(tableName: String, tableSchema: String)(implicit F: Bracket[F, Throwable]): F[List[Storage.Column]] = {
    (fr"SELECT column_name FROM information_schema.columns" ++
      fr"WHERE" ++
      fr"table_name = $tableName" ++
      fr"AND table_schema = $tableSchema" ++
      fr"ORDER BY ordinal_position").query[Storage.Column].to[List].transact(xa)
  }

  def getComment(tableName: String, tableSchema: String)(implicit F: Bracket[F, Throwable]) =
    (
      fr"SELECT C.oid, obj_description(C.oid) AS comment" ++
        fr"FROM pg_class" ++
        fr"C LEFT JOIN pg_namespace N ON N.oid = C.relnamespace" ++
        fr"WHERE C.relname = $tableName AND trim(N.nspname) = $tableSchema"
      ).query[Storage.Comment].option.transact(xa)
}

object Storage {

  case class Column(columnName: String)

  case class Comment(oid: Int, comment: Option[String])

  case class DbConfig(host: String,
                      port: Int,
                      dbname: String,
                      username: String,
                      password: String)

  def initialize[F[_]: Effect: ContextShift](storageConfig: DbConfig): Resource[F, Storage[F]] = storageConfig match {
    case DbConfig(host, port, dbname, username, password) =>
      val url = s"jdbc:postgresql://$host:$port/$dbname?loggerLevel=OFF"
      Blocker[F].map { blocker =>
        val xa = Transactor.fromDriverManager[F]("org.postgresql.Driver", url, username, password, blocker)
        Storage(xa)
      }
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
