package com.snowplowanalytics.iglu.ctl.commands

import cats.Show
import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema}
import com.snowplowanalytics.iglu.ctl._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Migrations}
import io.circe._

import java.nio.file.Path

object VerifyParquet {

  def process(command: Command.VerifyParquet): Result = {
    readSchemas(command.input)
      .map(handleInputSchemas)
      .map(prepareOutputMessage)
  }

  private def readSchemas(input: Path): FailingNel[List[SelfDescribingSchema[Schema]]] = {
    EitherT(File.readSchemas(input).map(Common.leftBiasedIor))
      .flatMap { files => 
        files.toList
          .traverse(schemaFile => toIgluSchema(schemaFile.content))
      }
  }

  private def handleInputSchemas(schemas: List[SelfDescribingSchema[Schema]]) = {
    groupSchemasByModel(schemas)
      .map(_.sortBy(_.self.schemaKey))
      .flatMap(detectBreakingChanges)
  }

  private def groupSchemasByModel(schemas: List[SelfDescribingSchema[Schema]]): List[List[SelfDescribingSchema[Schema]]] = {
    schemas
      .groupBy { schema =>
        (schema.self.schemaKey.vendor, schema.self.schemaKey.name, schema.self.schemaKey.version.model)
      }.values.toList
  }

  private def detectBreakingChanges(schemas: List[SelfDescribingSchema[Schema]]): List[BreakingChange] = {
    schemas.zip(schemas.tail)
      .filter(isBreaking)
      .map { breakingPair =>
        BreakingChange(breakingPair._1.self.schemaKey, breakingPair._2.self.schemaKey)
      }
  }

  private def isBreaking(pair: (SelfDescribingSchema[Schema], SelfDescribingSchema[Schema])): Boolean = {
    val source = Field.build(pair._1.self.schemaKey.toPath, pair._1.schema, enforceValuePresence = false)
    val target = Field.build(pair._2.self.schemaKey.toPath, pair._2.schema, enforceValuePresence = false)
    Migrations.isSchemaMigrationBreaking(source, target)
  }

  private def toIgluSchema(schema: SelfDescribingSchema[Json]): FailingNel[SelfDescribingSchema[Schema]] =
    EitherT.fromEither[IO] {
      Schema.parse(schema.schema)
        .map(SelfDescribingSchema(schema.self, _))
        .toRight(NonEmptyList.one(Common.Error.Message("Error while parsing schema jsons to Schema object")))
    }

  private def prepareOutputMessage(breakingChanges: List[BreakingChange]): List[String] = {
    if (breakingChanges.nonEmpty) {
      breakingChanges.map(_.show)
    } else {
      List("No breaking changes detected")
    }
  }

  private final case class BreakingChange(source: SchemaKey, target: SchemaKey)

  private implicit val breakingChangeShow: Show[BreakingChange] = Show.show { change =>
    s"Breaking change between '${change.source.toPath}' and '${change.target.toPath}'"
  }
  private implicit val schemaKeyOrdering: Ordering[SchemaKey] = SchemaKey.ordering

}
