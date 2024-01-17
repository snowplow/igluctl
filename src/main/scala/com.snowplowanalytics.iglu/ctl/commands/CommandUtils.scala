package com.snowplowanalytics.iglu.ctl.commands

import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.Order
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema}
import com.snowplowanalytics.iglu.ctl.Common.Error
import com.snowplowanalytics.iglu.ctl.{Common, Failing, Server}
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import org.http4s.client.Client

import java.util.UUID

object CommandUtils {

  implicit val schemaKeyOrdering: Order[SchemaKey] = Order.fromOrdering(SchemaKey.ordering)

  // Same vendor, name and model
  type SchemaFamily = NonEmptyList[SelfDescribingSchema[Schema]]

  def getAllSchemasFromServer(registryRoot: Server.HttpUrl,
                              readApiKey: Option[UUID],
                              httpClient: Client[IO]): Failing[NonEmptyList[IgluSchema]] = for {
    schemaJsons <- Pull.getSchemas(Server.buildPullRequest(registryRoot, readApiKey), httpClient)
    schemas <- EitherT.fromEither[IO](Generate.parseSchemas(schemaJsons))
    result <- EitherT.fromOption[IO](
      NonEmptyList.fromList(schemas), Common.Error.Message("No schema in the registry"): Error
    )
  } yield result

  def groupSchemasToFamilies(schemas: NonEmptyList[SelfDescribingSchema[Schema]]): List[SchemaFamily] = {
    schemas
      .groupBy { schema =>
        (schema.self.schemaKey.vendor, schema.self.schemaKey.name, schema.self.schemaKey.version.model)
      }
      .values
      .toList
      .map(_.sortBy(_.self.schemaKey))
  }
}
