package com.snowplowanalytics.iglu.ctl.commands

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO

import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel.RecoveryModel

import org.http4s.client.Client

import com.snowplowanalytics.iglu.ctl.{Command, Result}
import CommandUtils._

object VerifyRedshift {

  def process(command: Command.VerifyRedshift, httpClient: Client[IO]): Result = {
    getAllSchemasFromServer(command.igluServerUrl, command.apiKey, httpClient)
      .leftMap(NonEmptyList.one)
      .map(groupSchemasToFamilies)
      .map(_.flatMap(verifyFamily))
      .map(prepareOutputMessage)
  }

  private def verifyFamily(schemaFamily: SchemaFamily):  Option[SchemaCriterion] = {
    val recoveryModels = foldMapMergeRedshiftSchemas(schemaFamily).values.collect {
      case model: RecoveryModel => model
    }
    if (recoveryModels.nonEmpty) {
      val key = recoveryModels.head.schemaKey
      SchemaCriterion(key.vendor, key.name, key.format, key.version.model).some
    } else None
  }

  private def prepareOutputMessage(schemaCriterions: List[SchemaCriterion]): List[String] = {
    if (schemaCriterions.nonEmpty) {
      schemaCriterions.map(_.asString)
    } else {
      List("No breaking changes detected")
    }
  }
}
