package com.snowplowanalytics.iglu.ctl.commands

import cats.Show
import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.implicits._
import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.ctl._
import com.snowplowanalytics.iglu.ctl.Common.schemaKeyCatsOrder
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
    readSchemas(command.input).map(verify)
  }

  /** Pure core of the command: report the breaking changes within each schema family */
  private[ctl] def verify(schemas: NonEmptyList[SelfDescribingSchema[Schema]]): List[String] =
    prepareOutputMessage(handleInputSchemas(schemas))

  private def readSchemas(input: Path): FailingNel[NonEmptyList[SelfDescribingSchema[Schema]]] = {
    EitherT(File.readSchemas(input).map(Common.leftBiasedIor))
      .flatMap { files => 
        files
          .traverse(schemaFile => toIgluSchema(schemaFile.content))
      }
  }

  private def handleInputSchemas(schemas: NonEmptyList[SelfDescribingSchema[Schema]]): List[BreakingChange] = {
    groupSchemasToFamilies(schemas)
      .mapFilter(buildDdlFields)
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

  /**
    * A schema with no fields and `additionalProperties: false`, e.g. an event that carries no
    * payload, has no Parquet representation, so it cannot take part in a comparison and is
    * skipped. A family whose schemas are all skipped this way is skipped entirely.
    */
  private def buildDdlFields(schemaFamily: SchemaFamily): Option[NonEmptyList[Field]] = {
    NonEmptyList.fromList {
      schemaFamily
        .toList
        .mapFilter { schema =>
          Field.build(schema.self.schemaKey.toPath, schema.schema, enforceValuePresence = false)
        }
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

}
