package com.snowplowanalytics.iglu.ctl.commands

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iglu.schemaddl.redshift._
import org.http4s.client.Client
import com.snowplowanalytics.iglu.ctl.{Command, Result}
import CommandUtils._

object VerifyRedshift {

  def process(command: Command.VerifyRedshift, httpClient: Client[IO]): Result = {
    getAllSchemasFromServer(command.igluServerUrl, command.apiKey, httpClient)
      .leftMap(NonEmptyList.one)
      .map(groupSchemasToFamilies)
      .map(_.map(verifyFamily))
      .map(prepareOutputMessage(_, command.verbose))
  }

  private def verifyFamily(schemaFamily: SchemaFamily): Map[SchemaKey, NonEmptyList[String]] = {
    foldMapMergeRedshiftSchemas(schemaFamily)
      .recoveryModels
      .view
      .mapValues(_.errorAsStrings)
      .toMap
  }

  private def prepareOutputMessage(verifiedFamilies: List[Map[SchemaKey, NonEmptyList[String]]], verbose: Boolean): List[String] = {
    val result = verifiedFamilies.flatMap { family =>
      val optKey = family.headOption.map(_._1)
      val optCriterion = optKey.map(key =>  SchemaCriterion(key.vendor, key.name, key.format, key.version.model))
      optCriterion.fold[List[String]](Nil) { criterion =>
        if (verbose) {
          s"${criterion.asString}:" :: family.toList.sorted.map { case (sk, nel) =>
            s"  ${sk.toSchemaUri}: ${nel.mkString_("[", "," ,"]")}"
          }
        }
        else List(criterion.asString)
      }
    }
    if (result.isEmpty) List("No breaking changes detected") else result
  }
}
