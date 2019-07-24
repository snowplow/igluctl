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
package commands

import java.nio.file.{Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

import cats.data.{EitherNel, EitherT, NonEmptyList}
import cats.effect.IO
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl._
import com.snowplowanalytics.iglu.schemaddl.migrations.{Migration,FlatSchema}
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlFile, DdlGenerator, JsonPathGenerator}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.json4s.implicits._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

import com.snowplowanalytics.iglu.ctl.File.textFile
import com.snowplowanalytics.iglu.ctl.Utils.modelGroup
import com.snowplowanalytics.iglu.ctl.Common.Error

object Generate {

  /**
    * Primary working method of `generate` command
    * Get all JSONs from specified path, try to parse them as JSON Schemas,
    * convert to [[DdlOutput]] (combined table definition, JSON Paths, etc)
    * and output them to specified path, also output errors
    */
  def process(input: Path,
              output: Path,
              withJsonPaths: Boolean,
              rawMode: Boolean,
              dbSchema: String,
              varcharSize: Int,
              splitProduct: Boolean,
              noHeader: Boolean,
              force: Boolean,
              owner: Option[String]): Result = {

    val produce: NonEmptyList[IgluSchema] => DdlOutput =
      if (rawMode)
        transformVanilla(withJsonPaths, dbSchema, varcharSize, splitProduct, noHeader, owner)
      else
        transformSnowplow(withJsonPaths, dbSchema, varcharSize, splitProduct, noHeader, owner)

    for {
      _           <- File.checkOutput(output)
      schemaFiles <- EitherT(File.readSchemas(input).map(_.toEither))
      schemas     <- EitherT(
        IO.pure[EitherNel[Common.Error, NonEmptyList[IgluSchema]]](
          schemaFiles.traverse[Option, IgluSchema](
            s => Schema.parse(s.content.schema)
              .map(e => SelfDescribingSchema(s.content.self, e))
          ).toRight(NonEmptyList.one(Common.Error.Message("Error while parsing schema jsons to Schema object")))
        )
      )
      result      = produce(schemas)
      messages    <- EitherT(outputResult(output, result, force))
    } yield messages
  }

  /**
   * Class holding an aggregated output ready to be written
   * with all warnings collected due transformations
   *
   * @param ddls list of files with table definitions
   * @param migrations list of files with available migrations
   * @param jsonPaths list of JSONPaths files
   * @param warnings all warnings collected in process of parsing and
   *                 transformation
   */
  case class DdlOutput(ddls: List[File[String]], migrations: List[File[String]], jsonPaths: List[File[String]], warnings: List[String])

  def generateOutput(withJsonPaths: Boolean, rawMode: Boolean)
                    (migrations: List[TextFile],
                     tableDefinitions: List[TableDefinition]): DdlOutput = {
    val ddlWarnings = getDdlWarnings(tableDefinitions)

    // Build DDL-files and JSONPaths file (in correct order and camelCased column names)
    val (ddls, jsonPaths) = tableDefinitions
      .map(ddl => (makeDdlFile(ddl), if (withJsonPaths) Some(makeJsonPaths(rawMode, ddl)) else None))
      .unzip

    DdlOutput(ddls, migrations, jsonPaths.unite, warnings = ddlWarnings)
  }

  /**
   * Class holding all information for file with DDL
   *
   * @param path base directory for file
   * @param fileName DDL file name
   * @param ddlFile list of statements ready to be rendered
   * @param orderedSubSchemas subschemas which are ordered wrt to updates, nullness and alphabetic order
   */
  case class TableDefinition(path: String, fileName: String, ddlFile: DdlFile, orderedSubSchemas: OrderedSubSchemas)

  /** Dump warnings to stdout and collect errors and info messages for main method */
  def outputResult(output: Path, result: DdlOutput, force: Boolean): IO[EitherNel[Error, List[String]]] =
    for {
      _                  <- result.warnings.traverse_(printWarning)
      ddls                = result.ddls.map(_.setBasePath("sql")).map(_.setBasePath(output.toFile.getAbsolutePath))
      ddlsMessages       <- ddls.traverse[IO, Either[Error, String]](file => file.write(force))
      jsonPaths           = result.jsonPaths.map(_.setBasePath("jsonpaths")).map(_.setBasePath(output.toFile.getAbsolutePath))
      jsonPathsMessages  <- jsonPaths.traverse[IO, Either[Error, String]](file => file.write(force))
      migrations          = result.migrations.map(_.setBasePath("sql")).map(_.setBasePath(output.toFile.getAbsolutePath))
      migrationsMessages <- migrations.traverse[IO, Either[Error, String]](file => file.write(force))
    } yield (ddlsMessages ++ jsonPathsMessages ++ migrationsMessages).parTraverse(_.toEitherNel)

  /**
   * Get the file path and name from self-describing info
   * Like (com.mailchimp, subscribe_1)
   *
   * @param flatSelfElems all information from Self-describing schema
   * @return pair of relative filepath and filename
   */
  def getFileName(flatSelfElems: SchemaMap): (String, String) = {
    // Make the file name
    val version = "_".concat(flatSelfElems.schemaKey.version.asString.replaceAll("-[0-9]+-[0-9]+", ""))
    val file = flatSelfElems.schemaKey.name
                            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
                            .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
                            .replaceAll("-", "_")
                            .toLowerCase.concat(version)

    // Return the vendor and the file name together
    (flatSelfElems.schemaKey.vendor, file)
  }

  /** Log message either to stderr or stdout */
  private def printWarning(warning: String): IO[Unit] =
    IO(System.out.println(warning))

  /**
   * Aggregate list of Description-Definition pairs into map, so in value will be left
   * only table definition for latest revision-addition Schema
   * Use this to be sure vendor_tablename_1 always generated for latest Schema
   *
   * @param ddls list of pairs
   * @return Map with latest table definition for each Schema addition
   */
  private def groupWithLast(ddls: List[(SchemaMap, TableDefinition)]) = {
    val aggregated = ddls.foldLeft(Map.empty[ModelGroup, (SchemaMap, TableDefinition)]) {
      case (acc, (description, definition)) =>
        acc.get(modelGroup(description)) match {
          case Some((desc, _)) if desc.schemaKey.version.revision < description.schemaKey.version.revision =>
            acc ++ Map((modelGroup(description), (description, definition)))
          case Some((desc, _)) if desc.schemaKey.version.revision == description.schemaKey.version.revision &&
            desc.schemaKey.version.addition < description.schemaKey.version.addition =>
            acc ++ Map((modelGroup(description), (description, definition)))
          case None =>
            acc ++ Map((modelGroup(description), (description, definition)))
          case _ => acc
        }
    }
    aggregated.map { case (_, (desc, defn)) => (desc, defn) }
  }

  /**
   * Helper function used to extract warning from each generated DDL file
   * @todo make it more consistent with next DDL AST release
   */
  private[ctl] def getDdlWarnings(ddlFiles: List[TableDefinition]): List[String] = {
    def extract(definition: TableDefinition): List[String] = {
      val file = definition.ddlFile
      val igluUri = file.statements.collectFirst { case CommentOn(_, uri) => uri }
      file.warnings.map { warning =>
        igluUri match {
          case Some(uri) => s"Warning: in JSON Schema [$uri]: $warning"
          case None => s"Warning: in generated DDL [${definition.path}/${definition.fileName}]: $warning"
        }
      }
    }

    for { file <- ddlFiles; warning <- extract(file) } yield warning
  }

  // Self-describing

  /**
    * Transform list of self-describing JSON Schemas to a single [[DdlOutput]] containing
    * all data to produce: DDL files, JSONPath files, migrations, etc
    */
  private[ctl] def transformSnowplow(withJsonPaths: Boolean,
                                     dbSchema: String,
                                     varcharSize: Int,
                                     splitProduct: Boolean,
                                     noHeader: Boolean,
                                     owner: Option[String])
                                    (schemas: NonEmptyList[IgluSchema]): DdlOutput = {
    val migrationMap = Migration.buildMigrationMap(schemas.toList)
    val migrations = Migrations.reifyMigrationMap(migrationMap, Some(dbSchema), varcharSize)

    val orderedSubSchemaMap = Migration.buildOrderedSubSchemasMap(schemas.toList, migrationMap)
    val ddlFiles = orderedSubSchemaMap.map {
      case (schemaMap, subschemas) => produceTable(schemaMap, dbSchema, owner, varcharSize, false, noHeader)(subschemas)
    }.toList

    generateOutput(withJsonPaths, false)(migrations, ddlFiles)
  }

  // Header Section for a Redshift DDL File
  def redshiftDdlHeader = CommentBlock(Vector(
    s"AUTO-GENERATED BY ${generated.ProjectSettings.name} DO NOT EDIT",
    s"Generator: ${generated.ProjectSettings.name} ${generated.ProjectSettings.version}",
    s"Generated: ${ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))} UTC"
  ))

  // Header section with additional space
  def header(omit: Boolean) = if (omit) Nil else List(redshiftDdlHeader, Empty)


  // Raw

  /**
    * Transform list of self-describing JSON Schemas
    * to raw DDL output, without extra Snowplow columns
    */
  private[ctl] def transformVanilla(withJsonPaths: Boolean,
                                    dbSchema: String,
                                    varcharSize: Int,
                                    splitProduct: Boolean,
                                    noHeader: Boolean,
                                    owner: Option[String])
                                   (files: NonEmptyList[IgluSchema]): DdlOutput = {
    val ddlFiles = files.toList
      .map { schema =>
        val orderedSubSchemas = FlatSchema.order(FlatSchema.build(schema.schema).subschemas)
        produceTable(schema.self, dbSchema, owner, varcharSize, true, noHeader)(orderedSubSchemas)
      }
    generateOutput(withJsonPaths, true)(Nil, ddlFiles)
  }

  // Common

  /**
    * Produce table from flattened Schema and valid JSON Schema description
    */
  private def produceTable(schemaMap: SchemaMap,
                           dbSchema: String,
                           owner: Option[String],
                           varcharSize: Int,
                           rawMode: Boolean,
                           noHeader: Boolean)
                          (orderedSubSchemas: OrderedSubSchemas): TableDefinition = {
    val (path, filename) = getFileName(schemaMap)
    val tableName = StringUtils.getTableName(schemaMap)
    val schemaCreate = CreateSchema(dbSchema)
    val table = DdlGenerator.generateTableDdl(orderedSubSchemas, tableName, Some(dbSchema), varcharSize, rawMode)
    val commentOn = DdlGenerator.getTableComment(tableName, Some(dbSchema), schemaMap)
    val ddlFile = owner match {
      case Some(ownerStr) =>
        val owner = AlterTable(dbSchema + "." + tableName, OwnerTo(ownerStr))
        DdlFile(header(noHeader) ++ List(schemaCreate, Empty, table, Empty, commentOn, Empty, owner))
      case None => DdlFile(header(noHeader) ++ List(schemaCreate, Empty, table, Empty, commentOn))
    }
    TableDefinition(path, filename, ddlFile, orderedSubSchemas)
  }

  /**
    * Make Redshift DDL file out of table definition
    *
    * @param tableDefinition table definition object
    * @return text file with Redshift table DDL
    */
  private def makeDdlFile(tableDefinition: TableDefinition): File[String] = {
    textFile(Paths.get(tableDefinition.path, tableDefinition.fileName + ".sql"), tableDefinition.ddlFile.render(Nil))
  }

  /**
    * Make JSONPath file out of table definition
    *
    * @param ddl table definition
    * @return text file with JSON Paths if option is set
    */
  private def makeJsonPaths(rawMode: Boolean, ddl: TableDefinition): File[String] = {
    val content = JsonPathGenerator.getJsonPathsFile(ddl.orderedSubSchemas, rawMode)
    textFile(Paths.get(ddl.path, ddl.fileName + ".json"), content)
  }
}
