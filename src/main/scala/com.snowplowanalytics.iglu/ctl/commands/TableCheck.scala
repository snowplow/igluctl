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
import java.util.UUID

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import cats.effect._
import cats.Show

import fs2.Stream

import io.circe.syntax._
import io.circe._

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.ClientError

import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SelfDescribingSchema}

import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, StringUtils => SchemaDDLStringUtils}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList, FlatSchema}
import com.snowplowanalytics.iglu.schemaddl.redshift.{Column => DDLColumn}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlGenerator

import com.snowplowanalytics.iglu.ctl.{File, Server, Common, Storage, Command, Failing, Result => FinalResult}
import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.Storage.Column
import com.snowplowanalytics.iglu.ctl.commands.TableCheck.Result._


object TableCheck {

  /**
    * Represents result of one table check process
    */
  sealed trait Result extends Product with Serializable

  object Result {

    case class TableMatched(schema: SchemaKey) extends Result

    case class TableUnmatched(schema: SchemaKey, existingColumns: List[Column], expectedColumns: List[DDLColumn]) extends Result

    case class TableNotDeployed(schema: SchemaKey) extends Result

    case class CommentProblem(schema: SchemaKey, message: String) extends Result
  }

  /** Container to group all table check results in the end */
  case class AggregatedResult(matched: List[TableMatched],
                              unmatched: List[TableUnmatched],
                              notDeployed: List[TableNotDeployed],
                              commentProblems: List[CommentProblem])

  val aggregatedResultEmpty = AggregatedResult(Nil, Nil, Nil, Nil)

  implicit val tableCheckResultShow: Show[Result] = Show.show {
    case TableMatched(schemaKey) =>
      s"Table for ${schemaKey.toSchemaUri} is matched"
    case TableUnmatched(schemaKey, existingColumns, expectedColumns) =>
      s"""|Table for ${schemaKey.toSchemaUri} is not matched
         |  Existing columns: ${existingColumns.map(_.columnName).mkString(",")}
         |  Expected columns: ${expectedColumns.map(_.columnName).mkString(",")}""".stripMargin
    case TableNotDeployed(schemaKey) =>
      s"Table for ${schemaKey.toSchemaUri} is not deployed"
    case CommentProblem(schemaKey, message) =>
      s"Table for ${schemaKey.toSchemaUri}. $message"
  }

  implicit val aggregatedTableCheckResultsShow: Show[AggregatedResult] = Show.show {
    case AggregatedResult(matched, unmatched, notDeployed, commentProblems) =>
      s"""
         |${createSection("Not deployed:", notDeployed.map((_: Result).show).mkString("\n"))}
         |
         |${createSection("Matched:", matched.map((_: Result).show).mkString("\n"))}
         |
         |${createSection("Table comments:", commentProblems.map((_: Result).show).mkString("\n"))}
         |
         |${createSection("Unmatched:", unmatched.map((_: Result).show).mkString("\n"))}
         |
         |Unmatched: ${unmatched.length}, Comment issues: ${commentProblems.length}, Matched: ${matched.length}, Not Deployed: ${notDeployed.length}
       """.stripMargin.split("\n").filter(_.nonEmpty).mkString("\n")
  }

  /**
    * Primary method of table-check command. Checks whether table of the given schema
    * has expected structure which is determined with last and previous versions of
    * the given schema
    */
  def process(tableCheckType: Command.TableCheckType,
              dbschema: String,
              storageConfig: Command.DbConfig)(implicit cs: ContextShift[IO], t: Timer[IO]): FinalResult = {
    val stream = for {
      resolvedDbConfig <- Stream.eval(Storage.resolveDbConfig(storageConfig))
      storage <- Stream.resource(Storage.initialize[IO](resolvedDbConfig)).translate[IO, Failing](Common.liftIO)
      res     <- tableCheckType match {
        case Command.SingleTableCheck(resolver, schema) =>
          Stream.eval(tableCheckSingle(resolver, schema, storage, dbschema))
        case Command.MultipleTableCheck(igluServerUrl, apiKey) =>
          tableCheckMultiple(igluServerUrl, apiKey, storage, dbschema)
      }
    } yield res

    stream.compile.toList
      .leftMap(e => NonEmptyList.of(e))
      .map(l => List(aggregateResults(l).show))
  }

  /**
    * Checks corresponding table of given schemaKey against expected table definition
    * Expected table definition is created using model group schemas of given schemaKey
    * Returns whether corresponding table is matched or not in the end
    */
  def checkTable(storage: Storage[IO], lastVersion: SchemaKey, modelGroupSchemas: SchemaList, dbSchema: String): Failing[Result] = {
    val tableName = SchemaDDLStringUtils.getTableName(SchemaMap(lastVersion))
    val res = for {
      existingColumns <- storage.getColumns(tableName, dbSchema)
      comment         <- storage.getComment(tableName, dbSchema)
      commentCheck     = checkComment(comment, lastVersion).leftMap(msg => Result.CommentProblem(lastVersion, msg))
      expectedColumns  = buildTableDdl(modelGroupSchemas)
      matchCheck       = checkColumns(lastVersion, existingColumns, expectedColumns)
    } yield commentCheck.fold(identity, _ => matchCheck)
    EitherT.liftF(res)
  }

  def checkComment(comment: Option[Storage.Comment], key: SchemaKey): Either[String, Unit] =
    comment match {
      case None => ().asRight
      case Some(Storage.Comment(_, None)) => "Table comment is missing".asLeft
      case Some(Storage.Comment(_, Some(comment))) => SchemaKey.fromUri(comment) match {
        case Right(SchemaKey(vendor, name, _, _)) if key.vendor === vendor && key.name === name =>
          ().asRight
        case Right(schemaKey) =>
          s"SchemaKey found in table comment [${schemaKey.toSchemaUri}] does not match expected [${key.toSchemaUri}]".asLeft
        case Left(error) =>
          s"Invalid SchemaKey found [$comment]. ${error.code}".asLeft
      }
    }

  /**
    * Fetches model group schemas of given schemaKey with Iglu Client
    * and checks corresponding table structure against expected table
    * structure
    * Returns whether corresponding table is matched or not in the end
    */
  def tableCheckSingle(resolver: Path, schemaKey: SchemaKey, storage: Storage[IO], dbschema: String)(implicit t: Timer[IO]): Failing[Result] =
    for {
      schemas <- fetchSchemaModelGroup(resolver, schemaKey)
      res     <- checkTable(storage, schemaKey, schemas, dbschema)
    } yield res

  /**
    * Fetches all schemas from given Iglu registry and checks whether
    * corresponding tables of all the schemas is matching or not
    * Returns result stream of table check processes in the end
    */
  def tableCheckMultiple(registryRoot: Server.HttpUrl, readApiKey: Option[UUID], storage: Storage[IO], dbschema: String): Stream[Failing, Result] =
    for {
      schemas <- Stream.eval(getSchemas(registryRoot, readApiKey))
      schema <- Stream.emits[Failing, (SchemaKey, SchemaList)](schemas)
      (schemaKey, modelGroup) = schema
      res         <- Stream.eval(checkTable(storage, schemaKey, modelGroup, dbschema))
    } yield res

  def getSchemas(registryRoot: Server.HttpUrl, readApiKey: Option[UUID]): Failing[List[(SchemaKey, SchemaList)]] = {
    for {
      schemas <- SchemaList.fromFetchedSchemas[IO, Common.Error](
        {
          for {
            schemaJsons <- Pull.getSchemas(Server.buildPullRequest(registryRoot, readApiKey))
            schemas     <- EitherT.fromEither[IO](Generate.parseSchemas(schemaJsons))
            res <- EitherT.fromEither[IO](
              NonEmptyList.fromList(schemas) match {
                case None => (Common.Error.Message("No schema in the registry"): Common.Error).asLeft
                case Some(nel) => nel.asRight
              }
            )
          } yield res
        }
      )
    } yield schemas.toList.map { e =>
        val schemaKey = e match {
          case s: SchemaList.Single => s.schema.self.schemaKey
          case s: SchemaList.Full => s.schemas.last.self.schemaKey
        }
        (schemaKey, e)
      }
  }

  /**
    * Fetches model group schemas of given schemaKey with resolver which is created
    * from given resolver config
    */
  def fetchSchemaModelGroup(resolverPath: Path, schemaKey: SchemaKey)(implicit t: Timer[IO]): Failing[SchemaList] =
    for {
      resolverJson  <- EitherT(File.readFile(resolverPath).map(_.flatMap(_.asJson)))
      resolver      <- EitherT(Resolver.parse[IO](resolverJson.content))
        .leftMap(e => Error.ConfigParseError(s"Resolver can not created: $e"))
      schemaKeyList <- EitherT(resolver.listSchemas(schemaKey.vendor, schemaKey.name, schemaKey.version.model))
        .leftMap(e => Error.ServiceError(s"Error while lookup for schema key list: ${(e: ClientError).asJson.noSpaces}"))
      schemas       <- SchemaList.fromSchemaList(schemaKeyList, { schemaKey =>
          for {
            schemaJson  <- EitherT(resolver.lookupSchema(schemaKey))
              .leftMap(e => Error.ServiceError(s"Error while lookup for schema: ${(e: ClientError).asJson.noSpaces}"))
            schema      <- EitherT.fromEither[IO](parseSchema(schemaJson))
          } yield schema
        })
    } yield schemas

  def parseSchema(schema: Json): Either[Common.Error, IgluSchema] =
    SelfDescribingSchema.parse(schema)
      .leftMap(e => Common.Error.Message(s"Error while parsing schema jsons to Schema object, $e"))
      .flatMap { s =>
        Schema.parse(s.schema)
          .map(e => SelfDescribingSchema(s.self, e))
          .toRight(Common.Error.Message(s"Error while parsing schema jsons to Schema object"))
      }

  /** Creates table structure with given schemas */
  def buildTableDdl(schemas: SchemaList): List[DDLColumn] = {
    val name = SchemaDDLStringUtils.getTableName(schemas.latest)
    val orderedSubSchemas = FlatSchema.extractProperties(schemas)
    DdlGenerator.generateTableDdl(orderedSubSchemas, name, None, 4096, false).columns
  }

  /**
    * Compares existing and expected columns whether they are same or not.
    */
  private[ctl] def checkColumns(schemaKey: SchemaKey, existingColumns: List[Column], expectedColumns: List[DDLColumn]): Result =
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

  private def aggregateResults(results: List[Result]): AggregatedResult =
    results.foldLeft(aggregatedResultEmpty) {
      case (acc, result) => result match {
        case i: TableMatched => acc.copy(matched = i :: acc.matched)
        case i: TableUnmatched => acc.copy(unmatched = i :: acc.unmatched)
        case i: TableNotDeployed => acc.copy(notDeployed = i :: acc.notDeployed)
        case i: CommentProblem => acc.copy(commentProblems = i :: acc.commentProblems)
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
