/*
 * Copyright (c) 2016-2021 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.file.Path

// cats
import cats.syntax.either._

// circe
import io.circe._

// Iglu
import com.snowplowanalytics.iglu.core.SchemaMap
import com.snowplowanalytics.iglu.schemaddl.{ RevisionGroup, ModelGroup }

// This library
import File.splitPath

object Utils {

  /**
   * Get Iglu-compatible path (com.acme/event/jsonschema/1-0-2) from full
   * absolute file path
   *
   * @param fullPath file's absolute path
   * @return four last path entities joined by OS-separator
   */
  def getPath(fullPath: Path): String =
    splitPath(fullPath).takeRight(4).mkString("/")  // Always URL-compatible

  /**
   * Check if path of some JSON file corresponds with Iglu path extracted
   * from its self-describing info
   *
   * @param jsonFile some existing JSON file with defined path in it
   * @param schemaMap schema key extracted from it
   * @return true if extracted path is equal to FS path
   */
  def equalPath(jsonFile: File[Json], schemaMap: SchemaMap): Boolean = {
    val path = getPath(jsonFile.path.toAbsolutePath)
    SchemaMap.fromPath(path).toOption.contains(schemaMap)
  }

  /**
   * Extract from Schema description four elements defining REVISION
   *
   * @param schemaMap Schema description
   * @return tuple of four values defining revision
   */
  private[iglu] def revisionGroup(schemaMap: SchemaMap): RevisionGroup =
    (schemaMap.schemaKey.vendor,
      schemaMap.schemaKey.name,
      schemaMap.schemaKey.version.model,
      schemaMap.schemaKey.version.revision)

  /**
   * Extract from Schema description three elements defining MODEL
   *
   * @param schemaMap Schema description
   * @return tuple of three values defining revision
   */
  private[iglu] def modelGroup(schemaMap: SchemaMap): ModelGroup =
    (schemaMap.schemaKey.vendor, schemaMap.schemaKey.name, schemaMap.schemaKey.version.model)
}

