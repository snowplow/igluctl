/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu
package ctl

// Java
import java.nio.file.Paths

// Iglu core
import com.snowplowanalytics.iglu.core.SchemaMap

// Schema DDL
import com.snowplowanalytics.iglu.schemaddl._
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.MigrationGenerator.generateMigration

// This library
import File.textFile

/**
 * Helper module for [[Generate]] with functions responsible for DDL migrations
 */
object Migrations {
  /**
   * Transform whole MigrationMap with different schemas, models and revisions
   * to flat list of [[TextFile]]'s with their relative path
   * and stringified DDL as content
   *
   * @param migrationMap migration map of all Schemas created with buildMigrationMap
   * @return flat list of [[TextFile]] ready to be written
   */
  def reifyMigrationMap(
    migrationMap: MigrationMap,
    dbSchema: Option[String],
    varcharSize: Int): List[TextFile] =
    migrationMap.toList.flatMap {
      case (source, migrations) =>  createTextFiles(migrations.toList, source, varcharSize, dbSchema)
    }

  /**
   * Helper function creating list of [[TextFile]] (with same source, varcharSize
   * and dbSchema) from list of migrations
   */
  def createTextFiles(migrations: List[Migration], source: SchemaMap, varcharSize: Int, dbSchema: Option[String]) = {
    val baseFiles = migrations.map { migration =>
      textFile(Paths.get(migration.to.asString + ".sql"), generateMigration(migration, varcharSize, dbSchema).render)
    }

    baseFiles
      .map(_.setBasePath(source.schemaKey.version.asString))
      .map(_.setBasePath(source.schemaKey.name))
      .map(_.setBasePath(source.schemaKey.vendor))
  }
}
