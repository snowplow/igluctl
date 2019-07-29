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
import java.util.UUID

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import cats.effect._
import cats.data._
import cats.Show

import fs2.Stream

import io.circe.syntax._

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.ClientError

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer}

import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, StringUtils => SchemaDDLStringUtils}
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration
import com.snowplowanalytics.iglu.schemaddl.redshift.{CreateTable, Column => DDLColumn}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlGenerator

import com.snowplowanalytics.iglu.ctl.{File, Server}
import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.Storage.Column
import com.snowplowanalytics.iglu.ctl.{Common, Result, Storage}
import com.snowplowanalytics.iglu.ctl.Command
import com.snowplowanalytics.iglu.ctl.Failing
import com.snowplowanalytics.iglu.ctl.Utils.modelGroup
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.TableCheckResult._


object TableCheck {

  /**
    * Represents result of one table check process
    */
  sealed trait TableCheckResult extends Product with Serializable
  object TableCheckResult {
    /**
      * Represents cases where table has expected structure
      * @param schema schema which its corresponding table is
      *               checked against
      */
    case class TableMatched(schema: SchemaKey) extends TableCheckResult

    /**
      * Represents cases where table does not have expected structure
      * @param schema schema which its corresponding table is
      *               checked against
      * @param existingColumns existing columns of table
      * @param expectedColumns expected columns of table
      */
    case class TableUnmatched(schema: SchemaKey, existingColumns: List[Column], expectedColumns: List[DDLColumn]) extends TableCheckResult

    /**
      * Represents cases where expected table is not deployed
      * @param schema schema which its corresponding table is
      *               checked against
      */
    case class TableNotDeployed(schema: SchemaKey) extends TableCheckResult
  }

  /**
    * Container to collect all table check results in the end
    */
  case class AggregatedTableCheckResults(matchedResults: List[TableMatched] = List.empty,
                                         unmatchedResults: List[TableUnmatched] = List.empty,
                                         notDeployedResults: List[TableNotDeployed] = List.empty)

  implicit val tableCheckResultShow: Show[TableCheckResult] = Show.show {
    case TableMatched(schemaKey) =>
      s"Table for ${schemaKey.toSchemaUri} is matched"
    case TableUnmatched(schemaKey, existingColumns, expectedColumns) =>
      s"""|Table for ${schemaKey.toSchemaUri} is not matched
         |  Existing columns: ${existingColumns.map(_.columnName).mkString(",")}
         |  Expected columns: ${expectedColumns.map(_.columnName).mkString(",")}""".stripMargin
    case TableNotDeployed(schemaKey) =>
      s"Table for ${schemaKey.toSchemaUri} is not deployed"
  }

  implicit val aggregatedTableCheckResultsShow: Show[AggregatedTableCheckResults] = Show.show {
    case AggregatedTableCheckResults(matchedResults, unmatchedResults, notDeployedResults) =>
      s"""
         |${createSection("Not deployed:", notDeployedResults.map((_: TableCheckResult).show).mkString("\n"))}
         |
         |${createSection("Unmatched:", unmatchedResults.map((_: TableCheckResult).show).mkString("\n"))}
         |
         |${createSection("Matched:", matchedResults.map((_: TableCheckResult).show).mkString("\n"))}
         |
         |Total Matched: ${matchedResults.length}, Total Unmatched: ${unmatchedResults.length}, Total Not Deployed: ${notDeployedResults.length}
       """.stripMargin.split("\n").filter(_.nonEmpty).mkString("\n")
  }

  /**
    * Primary method of table-check command. Checks whether table of the given schema
    * has expected structure which is determined with last and previous versions of
    * the given schema
    */
  def process(tableCheckType: Command.TableCheckType,
              dbschema: String,
              storageConfig: Command.DbConfig)(implicit cs: ContextShift[IO], t: Timer[IO]): Result = {
    val stream = for {
      resolvedDbConfig <- Stream.eval(resolveDbConfig(storageConfig))
      storage <- Stream.resource(Storage.initialize[IO](resolvedDbConfig)).translate[IO, Failing](Common.liftIO)
      res     <- {
        val checkRes: Failing[List[TableCheckResult]] = tableCheckType match {
          case Command.SingleTableCheck(resolver, schema) =>
            tableCheckSingle(resolver, schema, storage, dbschema).map(r => List(r))
          case Command.MultipleTableCheck(igluServerUrl, apiKey) =>
            tableCheckMultiple(igluServerUrl, apiKey, storage, dbschema).compile.toList
        }
        Stream.eval(checkRes)
      }
    } yield res

    stream.compile.toList
      .leftMap(e => NonEmptyList.of(e))
      .map(l => List(aggregateResults(l.flatten).show))
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
  def resolveOptArgument(envVariable: String, optionalArg: Option[String]): IO[ValidatedNel[String, String]] =
    IO.delay {
      optionalArg match {
        case None => sys.env.get(envVariable).toValidNel[String](s"$envVariable is not set")
        case Some(arg) => arg.validNel[String]
      }
    }

  /**
    * Checks corresponding table of given schemaKey against expected table definition
    * Expected table definition is created using model group schemas of given schemaKey
    * Returns whether corresponding table is matched or not in the end
    */
  def checkTable(storage: Storage[IO], schemaKeyOfLastVersion: SchemaKey, modelGroupSchemas: List[IgluSchema], dbSchema: String): Failing[TableCheckResult] = {
    val res = for {
      existingColumns <- storage.getColumns(SchemaDDLStringUtils.getTableName(SchemaMap(schemaKeyOfLastVersion)), dbSchema)
      expectedColumns = buildTableDdl(modelGroupSchemas)
    } yield expectedColumns.map(checkColumns(schemaKeyOfLastVersion, existingColumns, _))
    EitherT(res)
  }

  /**
    * Fetches model group schemas of given schemaKey with Iglu Client
    * and checks corresponding table structure against expected table
    * structure
    * Returns whether corresponding table is matched or not in the end
    */
  def tableCheckSingle(resolver: Path, schemaKey: SchemaKey, storage: Storage[IO], dbschema: String)(implicit t: Timer[IO]): Failing[TableCheckResult] =
    for {
      schemas <- fetchSchemaModelGroup(resolver, schemaKey)
      res     <- checkTable(storage, schemaKey, schemas, dbschema)
    } yield res

  /**
    * Fetches all schemas from given Iglu registry and checks whether
    * corresponding tables of all the schemas is matching or not
    * Returns result stream of table check processes in the end
    */
  def tableCheckMultiple(registryRoot: Server.HttpUrl, readApiKey: Option[UUID], storage: Storage[IO], dbschema: String): Stream[Failing, TableCheckResult] =
    for {
      schemaJsons <- Stream.eval(Pull.getSchemas(Server.buildPullRequest(registryRoot, readApiKey)))
      schemas     <- Stream.eval(EitherT.fromEither[IO](Generate.parseSchemaJsonsToSchemas(schemaJsons)))
      modelGroupMap = createModelGroups(schemas)
      temp        <- Stream.emits[Failing, (SchemaKey, List[IgluSchema])](modelGroupMap.toList)
      (schemaKey, schemaModelGroup) = temp
      res         <- Stream.eval(checkTable(storage, schemaKey, schemaModelGroup, dbschema))
    } yield res

  /**
    * Fetches model group schemas of given schemaKey with resolver which is created
    * from given resolver config
    */
  def fetchSchemaModelGroup(resolverPath: Path, schemaKey: SchemaKey)(implicit t: Timer[IO]): Failing[List[IgluSchema]] =
    for {
      resolverJson <- EitherT(File.readFile(resolverPath).map(_.flatMap(_.asJson)))
      resolver     <- EitherT(Resolver.parse[IO](resolverJson.content))
        .leftMap(e => Error.ConfigParseError(s"Resolver can not created: $e"))
      schemaJsons  <- resolver.fetchSchemas(schemaKey.vendor, schemaKey.name, Some(schemaKey.version.model))
        .leftMap(e => Error.ServiceError(s"Error while lookup for schema: ${(e: ClientError).asJson.noSpaces}"))
        .map(_.filter(s => Ordering[SchemaVer].gteq(schemaKey.version, s.self.schemaKey.version)))
      schemas      <- EitherT.fromEither[IO](Generate.parseSchemaJsonsToSchemas(schemaJsons))
    } yield schemas

  /**
    * Creates table structure with given schemas
    */
  def buildTableDdl(schemas: List[IgluSchema]): Either[Common.Error, List[DDLColumn]] =
    NonEmptyList.fromList(schemas) match {
      case None => List.empty[DDLColumn].asRight
      case Some(nonEmptySchemas) =>
        val orderedSubSchemaMap = Migration.buildOrderedSubSchemasMap(nonEmptySchemas)
        val res = orderedSubSchemaMap.toList match {
          case (_, orderedSubSchemas)::Nil =>
            DdlGenerator.generateTableDdl(orderedSubSchemas, "", None, 4096, false).asRight[Common.Error]
          case Nil =>
            Error.Message("There is no ordered subschemas to generate table ddl").asLeft[CreateTable]
          case l =>
            Error.Message(s"There must be only one ordered subschemas to generate table ddl but there are multiple: ${l.map(_._1)}").asLeft[CreateTable]
        }
        res.map(_.columns)
    }

  /**
    * Compares existing and expected columns whether they are same or not.
    */
  private[ctl] def checkColumns(schemaKey: SchemaKey, existingColumns: List[Column], expectedColumns: List[DDLColumn]): TableCheckResult =
    existingColumns match {
      case Nil => TableNotDeployed(schemaKey)
      case _ =>
        expectedColumns.map(_.columnName).zipAll(existingColumns.map(_.columnName), "", "").foldLeft(List.empty[(String, String)]) {
          case (acc, (expected, existing)) =>
            if (expected != existing) (existing, expected) :: acc else acc
        } match {
          case Nil => TableMatched(schemaKey)
          case _ => TableUnmatched(schemaKey, existingColumns, expectedColumns)
        }
    }

  /**
    * Groups given schemas with their model groups
    * Keys of the map in the end are schemaKeys of last version
    * schemas of corresponding model group
    */
  private def createModelGroups(schemas: List[IgluSchema]): Map[SchemaKey, List[IgluSchema]] = {
    schemas
      .groupBy(s => modelGroup(s.self))
      .map {
        case (_, modelGroupSchemas) =>
          val lastVersionSchemaKey = modelGroupSchemas.maxBy(_.self.schemaKey.version).self.schemaKey
          (lastVersionSchemaKey, modelGroupSchemas)
      }
  }

  private def aggregateResults(results: List[TableCheckResult]): AggregatedTableCheckResults =
    results.foldLeft(AggregatedTableCheckResults()) {
      case (acc, result) => result match {
        case i: TableMatched => acc.copy(matchedResults = i :: acc.matchedResults)
        case i: TableUnmatched => acc.copy(unmatchedResults = i :: acc.unmatchedResults)
        case i: TableNotDeployed => acc.copy(notDeployedResults = i :: acc.notDeployedResults)
      }
    }

  private def createSection(sectionHeader: String, section: String): String = {
    if (section.isEmpty) ""
    else
      s"""
         |$sectionHeader
         |$section""".stripMargin
  }
}
