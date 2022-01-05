/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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

import java.util.UUID

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.ctl.{Result, Server}

object Keygen {
  def process(server: Server.HttpUrl, masterKey: UUID, prefix: Server.VendorPrefix): Result =
    Server.createKeys(server, masterKey, prefix)
      .map { case Server.ApiKeys(read, write) => List(s"Read key: $read", s"Write key: $write") }
      .leftMap(error => NonEmptyList.of(error))
}
