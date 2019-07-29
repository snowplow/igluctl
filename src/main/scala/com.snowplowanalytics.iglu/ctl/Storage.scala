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
package com.snowplowanalytics.iglu.ctl

import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.ExecutionContext

import cats.effect._

import doobie.implicits._
import doobie.util.transactor.Transactor

import com.snowplowanalytics.iglu.ctl.Command.DbConfig

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
}

object Storage {

  case class Column(columnName: String)

  def initialize[F[_]: Effect: ContextShift](storageConfig: DbConfig): Resource[F, Storage[F]] = storageConfig match {
    case DbConfig(host, port, dbname, username, password, driver, maxPoolSize) =>
      val url = s"jdbc:postgresql://$host:$port/$dbname"
      for {
        blockingContext <- {
          val alloc = Sync[F].delay(Executors.newFixedThreadPool(maxPoolSize))
          val free  = (es: ExecutorService) => Sync[F].delay(es.shutdown())
          Resource.make(alloc)(free).map(ExecutionContext.fromExecutor)
        }
        xa: Transactor[F] = Transactor.fromDriverManager[F](driver, url, username, password, blockingContext)
      } yield Storage(xa)
  }
}
