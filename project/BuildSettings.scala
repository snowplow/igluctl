/*
 * Copyright (c) 2016-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the
 * Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.  See the Apache License Version 2.0 for the specific
 * language governing permissions and limitations there under.
 */
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.AssemblyPlugin.defaultShellScript
import sbt._
import Keys._


object BuildSettings {
  ThisBuild / libraryDependencySchemes ++= Seq(
    "io.circe" %% "circe-jawn" % "always",
    "io.circe" %% "circe-core" % "always",
  )

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    scalacOptions         :=  Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Ywarn-unused-import",
      "-Ywarn-nullary-unit",
      "-Xfatal-warnings",
      "-Xlint",
      "-language:higherKinds",
      "-Ypartial-unification",
      "-Xfuture"),
    Compile / console / scalacOptions := Seq(
      "-deprecation",
      "-encoding", "UTF-8"
    ),
    Compile / doc / scalacOptions ++= Seq(
      "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
    ),
    javacOptions := Seq(
      "-source", "11",
      "-target", "11"
    ),
    Test / scalacOptions := Seq("-Yrangepos"),

    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.0" cross CrossVersion.full)
  )

  lazy val scalifySettings = Seq(
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.iglu.ctl.generated
                       |object ProjectSettings {
                       |  val version = "%s"
                       |  val name = "%s"
                       |}
                       |""".stripMargin.format(version.value, name.value, organization.value, scalaVersion.value))
      Seq(file)
    }.taskValue
  )

  // Assembly settings
  lazy val sbtAssemblySettings: Seq[Setting[_]] = Seq(

    // Executable jarfile
    assembly / assemblyOption ~= { _.copy(prependShellScript = Some(defaultShellScript)) },

    // Name it as an executable
    assembly / assemblyJarName := { name.value },

    // Make this executable
    assembly / mainClass := Some("com.snowplowanalytics.iglu.ctl.Main"),

    assembly / assemblyMergeStrategy := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case PathList("org", "apache", "commons", "logging", _@_*) => MergeStrategy.first
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
      case PathList("META-INF", "native-image", _@_*) => MergeStrategy.discard
      // case PathList("META-INF", _ @ _*) => MergeStrategy.discard    // Replaced with above for Stream Shredder
      case PathList("reference.conf", _@_*) => MergeStrategy.concat
      case PathList("codegen-resources", _@_*) => MergeStrategy.first // Part of AWS SDK v2
      case "mime.types" => MergeStrategy.first // Part of AWS SDK v2
      case "AUTHORS" => MergeStrategy.discard
      case PathList("org", "slf4j", "impl", _) => MergeStrategy.first
      case PathList("buildinfo", _) => MergeStrategy.first
      case x if x.contains("javax") => MergeStrategy.first
      case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first // http4s, 2.13 shim
      case x if x.endsWith("public-suffix-list.txt") => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

  lazy val buildSettings = basicSettings ++ scalifySettings ++ sbtAssemblySettings
}
