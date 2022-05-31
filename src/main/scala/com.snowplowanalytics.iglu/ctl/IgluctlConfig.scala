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
package com.snowplowanalytics.iglu.ctl

import cats.effect.IO
import com.snowplowanalytics.iglu.ctl.IgluctlConfig.IgluctlAction.S3Cp
import com.snowplowanalytics.iglu.ctl.IgluctlConfig.IgluctlAction.Push
import org.http4s.client.Client

/** Common configuration format used for `static deploy` command */
case class IgluctlConfig(description: Option[String],
                         lint: Command.Lint,
                         generate: Command.StaticGenerate,
                         actions: List[IgluctlConfig.IgluctlAction])

object IgluctlConfig {
  /** Trait common to S3cp and Push commands */
  sealed trait IgluctlAction

  def process(httpClient: Client[IO], config: IgluctlAction): Result = {
    config match {
      case S3Cp(command) => commands.S3cp.process(command)
      case Push(command) => commands.Push.process(command, httpClient)
    }
  }
  object IgluctlAction {
    case class S3Cp(command: Command.StaticS3Cp) extends IgluctlAction
    case class Push(command: Command.StaticPush) extends IgluctlAction
  }
}
