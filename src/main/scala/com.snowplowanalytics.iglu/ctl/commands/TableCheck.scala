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

import java.nio.file.Path

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import cats.effect.{ContextShift, IO}

import fs2.Stream

import io.circe.syntax._

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.ClientError

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer}

import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, StringUtils => SchemaDDLStringUtils}
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration
import com.snowplowanalytics.iglu.schemaddl.redshift.{CreateTable, Column => DDLColumn}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlGenerator

import com.snowplowanalytics.iglu.ctl.File
import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.Storage.Column
import com.snowplowanalytics.iglu.ctl.{Common, Result, Storage}
import com.snowplowanalytics.iglu.ctl.Command.DbConfig
import com.snowplowanalytics.iglu.ctl.IOInstances._

object TableCheck {

  /**
    * Primary method of table-check command. Checks whether table of the given schema
    * has expected structure which is determined with last and previous versions of
    * the given schema
    */
  def process(resolver: Path, schema: SchemaKey, dbschema: String, storageConfig: DbConfig): Result = {
    import scala.concurrent.ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(global)

    val stream = for {
      storage         <- Stream.resource(Storage.initialize[IO](storageConfig))
      existingColumns <- Stream.eval(storage.getColumns(SchemaDDLStringUtils.getTableName(SchemaMap(schema)), dbschema))
      expectedColumns <- Stream.eval(createColumnsFromSchema(resolver, schema, dbschema).value)
    } yield expectedColumns.flatMap(checkColumns(existingColumns, _))

    EitherT(stream.compile.toList.map(_.sequence[Either[Common.Error, ?], Unit]))
      .leftMap(e => NonEmptyList.of(e))
      .map(_ => List("Table matched with given schema"))
  }

  /**
    * Build expected table structure with given schema key.
    * Initially, it creates resolver with given resolver config and fetches
    * necessary schemas with given schema key. After, it builds table structure
    * using these schemas
    */
  def createColumnsFromSchema(resolverPath: Path, schemaKey: SchemaKey, dbSchema: String): EitherT[IO, Common.Error, List[DDLColumn]] = {
    for {
      resolverJson <- EitherT(File.readFile(resolverPath).map(_.flatMap(_.asJson)))
      resolver     <- EitherT(Resolver.parse[IO](resolverJson.content))
        .leftMap(e => Error.ConfigParseError(s"Resolver can not created: $e"))
      schemaJsons  <- resolver.fetchSchemas(schemaKey.vendor, schemaKey.name, Some(schemaKey.version.model))
        .leftMap(e => Error.ServiceError(s"Error while lookup for schema: ${(e: ClientError).asJson.noSpaces}"))
        .map(_.filter(s => Ordering[SchemaVer].gteq(schemaKey.version, s.self.schemaKey.version)))
      schemas      <- EitherT.fromEither[IO](
          Generate.parseSchemaJsonsToSchemas(schemaJsons)
            .toRight(Common.Error.Message("Error while parsing schema jsons to Schema object"))
        )
      tableDdl     <- EitherT.fromEither[IO](buildTableDdl(schemas, dbSchema))
    } yield tableDdl.columns
  }

  /**
    * Creates table structure with given schemas
    */
  def buildTableDdl(schemas: List[IgluSchema], dbSchema: String): Either[Common.Error, CreateTable] = {
    val migrationMap = Migration.buildMigrationMap(schemas)
    val orderedSubSchemaMap = Migration.buildOrderedSubSchemasMap(schemas, migrationMap)
    orderedSubSchemaMap.toList match {
      case (_, orderedSubSchemas)::Nil =>
        DdlGenerator.generateTableDdl(orderedSubSchemas, "", Some(dbSchema), 4096, false).asRight[Common.Error]
      case Nil =>
        Error.Message("There is no ordered subschemas to generate table ddl").asLeft[CreateTable]
      case l =>
        Error.Message(s"There must be only one ordered subschemas to generate table ddl but there are multiple: ${l.map(_._1)}").asLeft[CreateTable]
    }
  }

  /**
    * Compares existing and expected columns whether they are same or not.
    */
  private[ctl] def checkColumns(existingColumns: List[Column], expectedColumns: List[DDLColumn]): Either[Common.Error, Unit] = {
    expectedColumns.map(_.columnName).zipAll(existingColumns.map(_.columnName), "", "").foldLeft(List.empty[(String, String)]) {
      case (acc, (expected, existing)) =>
        if (!expected.equals(existing)) (existing, expected) :: acc else acc
    } match {
      case Nil => ().asRight[Common.Error]
      case l => Error.Message(
        s"""
           |Existing and expected columns are different:
           |  existing: ${existingColumns.map(_.columnName).mkString(",")}
           |  expected: ${expectedColumns.map(_.columnName).mkString(",")}""".stripMargin
      ).asLeft[Unit]
    }
  }
}
