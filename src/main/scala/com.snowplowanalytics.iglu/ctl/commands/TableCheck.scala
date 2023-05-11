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

import cats.data.{EitherT, NonEmptyList}
import cats.effect._
import cats.syntax.eq._
import cats.syntax.show._
import cats.syntax.traverse._
import cats.{Order, Show}
import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema}
import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.Storage.Column
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.Result._
import com.snowplowanalytics.iglu.ctl.{Command, Common, Failing, File, Server, Storage, Result => FinalResult}
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel.GoodModel
import com.snowplowanalytics.iglu.schemaddl.redshift.getFinalMergedModel
import io.circe._
import io.circe.syntax._
import org.http4s.client.Client

import java.nio.file.Path
import java.util.UUID


object TableCheck {

  def process(command: Command.TableCheck, httpClient: Client[IO])
             (implicit cs: ContextShift[IO], t: Timer[IO]): FinalResult =
    withBlocker { blocker =>
      createStorage(command, blocker)
        .flatMap(storage => handleCommand(command, httpClient, storage))
        .leftMap(e => NonEmptyList.of(e))
        .map(l => List(aggregateResults(l).show))
    }

  private def handleCommand(command: Command.TableCheck,
                            httpClient: Client[IO],
                            storage: Storage[IO])
                           (implicit t: Timer[IO]): Failing[List[Result]] = {
    command.tableCheckType match {
      case Command.SingleTableCheck(resolver, schema) =>
        tableCheckSingle(resolver, schema, storage, command.dbSchema)
      case Command.MultipleTableCheck(igluServerUrl, apiKey) =>
        tableCheckMultiple(igluServerUrl, apiKey, storage, command.dbSchema, httpClient)
    }
  }

  private def tableCheckSingle(resolver: Path,
                               schemaKey: SchemaKey,
                               storage: Storage[IO],
                               dbschema: String)(implicit t: Timer[IO]): Failing[List[Result]] =
    for {
      schemas <- fetchSchemaFamily(resolver, schemaKey)
      result <- checkTable(storage, schemas, dbschema)
    } yield List(result)

  private def tableCheckMultiple(registryRoot: Server.HttpUrl,
                                 readApiKey: Option[UUID],
                                 storage: Storage[IO],
                                 dbschema: String,
                                 httpClient: Client[IO]): Failing[List[Result]] =
    for {
      schemas <- getAllSchemasFromServer(registryRoot, readApiKey, httpClient)
      result <- groupSchemasToFamilies(schemas).traverse { family =>
        checkTable(storage, family, dbschema)
      }
    } yield result

  private def checkTable(storage: Storage[IO],
                         schemaFamily: NonEmptyList[IgluSchema],
                         dbSchema: String): Failing[Result] = EitherT.liftF {
    val ddlModel = getFinalMergedModel(schemaFamily)
    val expectedColumns = prepareExpectedColumns(ddlModel)
  
    for {
      existingColumns <- storage.getColumns(ddlModel.tableName, dbSchema)
      existingComment <- storage.getComment(ddlModel.tableName, dbSchema)
      lastVersion = schemaFamily.last.self.schemaKey
    } yield verifyExistingStorage(lastVersion, existingComment, existingColumns, expectedColumns)
  }

  private def prepareExpectedColumns(ddlModel: GoodModel): List[Column] = {
    ddlModel
      .entries
      .map { entry =>
        Storage.Column(entry.columnName, entry.columnType, entry.isNullable)
      }
  }

  private[ctl] def verifyExistingStorage(latestSchemaKey: SchemaKey,
                                         existingComment: Option[Storage.Comment],
                                         existingColumns: List[Column],
                                         expectedColumns: List[Column]): Result = {
    existingColumns match {
      case Nil => TableNotDeployed(latestSchemaKey)
      case _ =>
        val columnIssues = detectColumnIssues(existingColumns, expectedColumns)
        val commentIssues = checkComment(existingComment, latestSchemaKey).toList
        
        NonEmptyList.fromList(commentIssues ::: columnIssues) match {
          case Some(discoveredIssues) =>
            TableUnmatched(latestSchemaKey, discoveredIssues, expectedColumns, existingColumns)
          case None =>
            TableMatched(latestSchemaKey) 
        }
    }
  }

  private def detectColumnIssues(existingColumns: List[Column],
                                 expectedColumns: List[Column]): List[TableIssue] = {
    val existingByName = columnsByName(existingColumns)
    val expectedByName = columnsByName(expectedColumns)

    findTypesIssues(existingByName, expectedByName) ++
      findAdditionalColumns(existingByName, expectedByName) ++
      findMissingColumns(existingByName, expectedByName)
  }

  private def findTypesIssues(existingByName: Map[String, Column],
                              expectedByName: Map[String, Column]): List[TableIssue.ColumnMismatch] = {
    existingByName.keySet.intersect(expectedByName.keySet)
      .map(name => (existingByName(name), expectedByName(name)))
      .flatMap {
        case (existing, expected) if expected != existing =>
          Some(TableIssue.ColumnMismatch(expected, existing))
        case _ =>
          None
      }
      .toList
  }

  private def findAdditionalColumns(existingByName: Map[String, Column],
                                    expectedByName: Map[String, Column]): List[TableIssue.AdditionalColumnInStorage] = {
    existingByName.keySet.diff(expectedByName.keySet)
      .map(name => TableIssue.AdditionalColumnInStorage(existingByName(name)))
      .toList
  }

  private def findMissingColumns(existingByName: Map[String, Column],
                                 expectedByName: Map[String, Column]): List[TableIssue.MissingColumnInStorage] = {
    expectedByName.keySet.diff(existingByName.keySet)
      .map(name => TableIssue.MissingColumnInStorage(expectedByName(name)))
      .toList
  }

  private def columnsByName(existingColumns: List[Column]): Map[String, Column] = {
    existingColumns.map(column => (column.name, column)).toMap
  }

  private def checkComment(comment: Option[Storage.Comment],
                           key: SchemaKey): Option[TableIssue.CommentProblem] = {
    comment match {
      case Some(Storage.Comment(comment)) => SchemaKey.fromUri(comment) match {
        case Right(schemaKey) if schemaKey === key  =>
          None
        case Right(schemaKey) =>
          Some(TableIssue.CommentProblem(s"SchemaKey found in table comment [${schemaKey.toSchemaUri}] does not match expected [${key.toSchemaUri}]"))
        case Left(error) =>
          Some(TableIssue.CommentProblem(s"Invalid SchemaKey found [$comment]. ${error.code}"))
      }
      case None =>
        Some(TableIssue.CommentProblem("Table comment is missing"))
    }
  }

  private def fetchSchemaFamily(resolverPath: Path,
                                schemaKey: SchemaKey)
                               (implicit t: Timer[IO]): Failing[NonEmptyList[IgluSchema]] =
    for {
      resolver <- createResolver(resolverPath)
      jsons <- resolver.fetchSchemas(schemaKey.vendor, schemaKey.name, schemaKey.version.model)
        .leftMap(e => Error.ServiceError(s"Error while lookup for schema family: ${schemaKey.vendor}-${schemaKey.name}-${schemaKey.version.model}, ${(e: ClientError).asJson.noSpaces}"))
      schemas <- EitherT.fromEither[IO](jsons.traverse(parseSchema))
      schemasNel <- EitherT.fromOption[IO](
        NonEmptyList.fromList(schemas), Error.ServiceError(s"No schemas found for family: ${schemaKey.vendor}-${schemaKey.name}-${schemaKey.version.model}"): Error
      )
    } yield schemasNel

  private def getAllSchemasFromServer(registryRoot: Server.HttpUrl,
                                      readApiKey: Option[UUID],
                                      httpClient: Client[IO]): Failing[NonEmptyList[IgluSchema]] = for {
    schemaJsons <- Pull.getSchemas(Server.buildPullRequest(registryRoot, readApiKey), httpClient)
    schemas <- EitherT.fromEither[IO](Generate.parseSchemas(schemaJsons))
    result <- EitherT.fromOption[IO](
      NonEmptyList.fromList(schemas), Common.Error.Message("No schema in the registry"): Error
    )
  } yield result

  private def groupSchemasToFamilies(schemas: NonEmptyList[SelfDescribingSchema[Schema]]): List[NonEmptyList[SelfDescribingSchema[Schema]]] = {
    schemas
      .groupBy { schema =>
        (schema.self.schemaKey.vendor, schema.self.schemaKey.name, schema.self.schemaKey.version.model)
      }
      .values
      .toList
      .map(_.sortBy(_.self.schemaKey))
  }


  private def parseSchema(schema: SelfDescribingSchema[Json]): Either[Common.Error, IgluSchema] = {
    Schema.parse(schema.schema)
      .map(e => SelfDescribingSchema(schema.self, e))
      .toRight(Common.Error.Message(s"Error while parsing schema jsons to Schema object"))
  }


  private def aggregateResults(results: List[Result]): AggregatedResult =
    results.foldLeft(AggregatedResult(Nil, Nil, Nil)) {
      case (acc, result) => result match {
        case i: TableMatched => acc.copy(matched = i :: acc.matched)
        case i: TableUnmatched => acc.copy(unmatched = i :: acc.unmatched)
        case i: TableNotDeployed => acc.copy(notDeployed = i :: acc.notDeployed)
      }
    }

  private def createResolver(resolverPath: Path): Failing[Resolver[IO]] =
    for {
      resolverJson <- EitherT(File.readFile(resolverPath).map(_.flatMap(_.asJson)))
      resolver <- EitherT(Resolver.parse[IO](resolverJson.content))
        .leftMap(e => Error.ConfigParseError(s"Resolver can not created: $e"): Error)
    } yield resolver

  private def createStorage(command: Command.TableCheck, blocker: Blocker)
                           (implicit cs: ContextShift[IO]): Failing[Storage[IO]] = {
    Storage.resolveDbConfig(command.storageConfig)
      .map(config => Storage.initialize[IO](config, blocker))
  }

  private def withBlocker(process: Blocker => FinalResult)
                         (implicit cs: ContextShift[IO]): FinalResult = EitherT {
    Blocker[IO].use { blocker =>
      process(blocker).value
    }
  }


  sealed trait Result extends Product with Serializable

  object Result {

    case class TableNotDeployed(schema: SchemaKey) extends Result

    case class TableMatched(schema: SchemaKey) extends Result

    case class TableUnmatched(schema: SchemaKey,
                              issues: NonEmptyList[TableIssue],
                              allExpectedColumns: List[Column],
                              allExistingColumns: List[Column]) extends Result

    sealed trait TableIssue

    object TableIssue {
      final case class CommentProblem(message: String) extends TableIssue

      final case class ColumnMismatch(expected: Column, existing: Column) extends TableIssue

      final case class MissingColumnInStorage(expected: Column) extends TableIssue

      final case class AdditionalColumnInStorage(existing: Column) extends TableIssue
    }
  }

  private case class AggregatedResult(matched: List[TableMatched],
                                      unmatched: List[TableUnmatched],
                                      notDeployed: List[TableNotDeployed])

  private implicit val tableCheckResultShow: Show[Result] = Show.show {
    case TableMatched(schemaKey) =>
      s"Table for ${schemaKey.toSchemaUri} is matched"
    case TableUnmatched(schemaKey, issues, expected, existing) =>
      s"""Table for ${schemaKey.toSchemaUri} is not matched. Issues:\n${issues.map(_.show).toList.mkString("\n")}
         |-----------------
         |Expected columns:\n${expected.map(_.show).mkString("\n")}
         |-----------------
         |Existing columns:\n${existing.map(_.show).mkString("\n")}
         |""".stripMargin
    case TableNotDeployed(schemaKey) =>
      s"Table for ${schemaKey.toSchemaUri} is not deployed"
  }

  private implicit val issueShow: Show[TableIssue] = Show.show {
    case TableIssue.CommentProblem(message) =>
      s"* Comment problem - $message"
    case TableIssue.ColumnMismatch(expected, existing) =>
      s"* Column doesn't match, expected: ${expected.show}, actual: ${existing.show}"
    case TableIssue.MissingColumnInStorage(expected) =>
      s"* Column existing in the schema but is not present in the storage: ${expected.show}"
    case TableIssue.AdditionalColumnInStorage(existing) =>
      s"* Column existing in the storage but is not defined in the schema: ${existing.show}"
  }

  private implicit val columnShow: Show[Column] = Show.show(column => s"'${column.name} ${column.`type`.show}${if (column.isNullable) "" else " NOT NULL"}'")

  private implicit val aggregatedTableCheckResultsShow: Show[AggregatedResult] = Show.show {
    case AggregatedResult(matched, unmatched, notDeployed) =>
      s"""
         |${createOutputSection("Not deployed:", notDeployed.map((_: Result).show).mkString("\n"))}
         |${createOutputSection("Matched:", matched.map((_: Result).show).mkString("\n"))}
         |${createOutputSection("Unmatched:", unmatched.map((_: Result).show).mkString("\n"))}
         |Unmatched: ${unmatched.length}, Matched: ${matched.length}, Not Deployed: ${notDeployed.length}
         |""".stripMargin.split("\n").filter(_.nonEmpty).mkString("\n")
  }

  private def createOutputSection(sectionHeader: String, section: String): String = {
    if (section.isEmpty) ""
    else
      s"""
         |$sectionHeader
         |$section
         |----------------------""".stripMargin
  }

  private implicit val schemaKeyOrdering: Order[SchemaKey] = Order.fromOrdering(SchemaKey.ordering)

}
