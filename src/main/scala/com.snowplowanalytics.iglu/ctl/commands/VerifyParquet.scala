package com.snowplowanalytics.iglu.ctl.commands

import cats.{Order, Show}
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

  // Same vendor, name and model
  private type SchemaFamily = NonEmptyList[SelfDescribingSchema[Schema]]

  private final case class BreakingChange(source: Field, changes: List[Migrations.Breaking])

  def process(command: Command.VerifyParquet): Result = {
    readSchemas(command.input)
      .map(handleInputSchemas)
      .map(prepareOutputMessage)
  }

  private def readSchemas(input: Path): FailingNel[NonEmptyList[SelfDescribingSchema[Schema]]] = {
    EitherT(File.readSchemas(input).map(Common.leftBiasedIor))
      .flatMap { files => 
        files
          .traverse(schemaFile => toIgluSchema(schemaFile.content))
      }
  }

  private def handleInputSchemas(schemas: NonEmptyList[SelfDescribingSchema[Schema]]) = {
    groupSchemasToFamilies(schemas)
      .map(buildDdlFields)
      .flatMap(detectBreakingChanges)
  }

  private def groupSchemasToFamilies(schemas: NonEmptyList[SelfDescribingSchema[Schema]]): List[SchemaFamily] = {
    schemas
      .groupBy { schema =>
        (schema.self.schemaKey.vendor, schema.self.schemaKey.name, schema.self.schemaKey.version.model)
      }
      .values
      .toList
      .map(_.sortBy(_.self.schemaKey))
  }

  private def buildDdlFields(schemaFamily: SchemaFamily): NonEmptyList[Field] = {
    schemaFamily
      .map { schema =>
        Field.build(schema.self.schemaKey.toPath, schema.schema, enforceValuePresence = false)
      }
  }

  private def detectBreakingChanges(fields: NonEmptyList[Field]): List[BreakingChange] = {
    def go(source: Field, pendingFields: List[Field]): List[BreakingChange] = {
      pendingFields match {
        case Nil => List.empty 
        case currentTarget :: others =>
          Migrations.mergeSchemas(source, currentTarget) match {
            case Right(merged) => go(merged, others)
            case Left(breakingChanges) => BreakingChange(currentTarget, breakingChanges) :: go(source, others) 
          } 
      }
    }

    go(fields.head, fields.tail)
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
  
  private implicit val breakingChangeShow: Show[BreakingChange] = Show.show { change =>
    s"Breaking change introduced by '${change.source.name}'. Changes: ${change.changes.map(_.toString).mkString("\n")}"
  }
  private implicit val schemaKeyOrdering: Order[SchemaKey] = Order.fromOrdering(SchemaKey.ordering)

}
