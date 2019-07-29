/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import java.util.UUID
import java.nio.file.Path

import com.snowplowanalytics.iglu.core.SchemaKey

// cats
import cats.data.{ ValidatedNel, Validated }
import cats.implicits._

// decline
import com.monovore.decline.{ Command => Cmd, _ }

// Schema DDL
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter

object Command {

  def parse(args: List[String]) =
    igluctlCommand.parse(args)

  implicit val readUuid: Argument[UUID] = new Argument[UUID] {
    def read(string: String): ValidatedNel[String, UUID] =
      Validated.catchOnly[IllegalArgumentException](UUID.fromString(string)).leftMap(_.getMessage).toValidatedNel
    def defaultMetavar: String = "uuid"
  }

  implicit val readLinters: Argument[List[Linter]] = new Argument[List[Linter]] {
    def read(string: String): ValidatedNel[String, List[Linter]] =
      commands.Lint.parseOptionalLinters(string).leftMap(_.show).toValidatedNel
    def defaultMetavar: String = "linters"
  }

  implicit val readRegistryRoot: Argument[Server.HttpUrl] = new Argument[Server.HttpUrl] {
    def read(string: String): ValidatedNel[String, Server.HttpUrl] =
      Server.HttpUrl.parse(string).leftMap(_.show).toValidatedNel
    def defaultMetavar: String = "uri"
  }

  implicit val readSchemaKey: Argument[SchemaKey] = new Argument[SchemaKey] {
    override def read(string: String): ValidatedNel[String, SchemaKey] =
      SchemaKey.fromUri(string).leftMap(_.code).toValidatedNel
    override def defaultMetavar: String = "schemaKey"
  }

  // common options
  val input = Opts.argument[Path]("input")

  // static generate options
  val output = Opts.argument[Path]("output")
  val dbschema = Opts.option[String]("dbschema", "Redshift schema name", metavar = "name").withDefault("atomic")
  val owner = Opts.option[String]("set-owner", "Redshift table owner", metavar = "name").orNone
  val varcharSize = Opts.option[Int]("varchar-size", "Default size for varchar data type", metavar = "n").withDefault(4096)
  val withJsonPathsOpt = Opts.flag("with-json-paths", "Produce JSON Paths files with DDL").orFalse
  val rawMode = Opts.flag("raw-mode", "Produce raw DDL without Snowplow-specific data").orFalse
  val splitProduct = Opts.flag("raw-mode", "Split product types (e.g. [string,integer]) into separate columns").orFalse
  val noHeader = Opts.flag("no-header", "Do not place header comments into output DDL").orFalse
  val force = Opts.flag("force", "Force override existing manually-edited files").orFalse

  // static push options
  val registryRoot = Opts.argument[Server.HttpUrl]("uri")
  val apikey = Opts.argument[UUID]("uuid")
  val public = Opts.flag("public", "Upload schemas as public").orFalse

  // static s3cp options
  type S3Path = String
  type Bucket = String
  val bucket = Opts.argument[Bucket]("bucket")
  val s3path = Opts.option[S3Path]("s3path", "Path in the bucket to upload Schemas").orNone
  val accessKeyId = Opts.option[String]("accessKeyId", "AWS Access Key Id", metavar = "key").orNone
  val secretAccessKey = Opts.option[String]("secretAccessKey", "AWS Secret Access Key", metavar = "key").orNone
  val profile = Opts.option[String]("profile", "AWS Profile name", metavar = "name").orNone
  val region = Opts.option[String]("region", "AWS Region", metavar = "name").orNone

  // lint options
  val lintersListText =
    "rootObject           - Check that root of schema has object type and contains properties\n" +
    "unknownFormats       - Check that schema doesn't contain unknown formats\n" +
    "numericMinMax        - Check that schema with numeric type contains both minimum and maximum properties\n" +
    "stringLength         - Check that schema with string type contains maxLength property or other ways to extract max length\n" +
    "optionalNull         - Check that non-required fields have null type\n" +
    "stringMaxLengthRange - Check that possible VARCHAR is in acceptable limits for Redshift\n" +
    "description          - Check that property contains description"
  val skipWarnings = Opts.flag("skip-warnings", "Don't output messages with log level less than ERROR").orFalse
  val skipChecks = Opts.option[List[Linter]]("skip-checks", s"Lint without specified linters, given comma separated\n$lintersListText").withDefault(List.empty)

  // server keygen options
  val vendorPrefix = Opts.option[String]("vendor-prefix", "Vendor prefix to associate with generated key")
    .mapValidated(s => Server.VendorPrefix.fromString(s).toValidatedNel).withDefault(Server.VendorPrefix.Wildcard)

  // database options
  val dbPort = Opts.option[Int]("port", "Database port").withDefault(5439)
  val dbHost = Opts.option[String]("host", "Database host address").orNone
  val dbName = Opts.option[String]("dbname", "Database name").orNone
  val dbUserName = Opts.option[String]("username", "Database username").orNone
  val dbPassword = Opts.option[String]("password", "Database password").orNone
  val dbConfig = (dbHost, dbPort, dbName, dbUserName, dbPassword).mapN(DbConfig.apply)

  // TableCheck options
  val igluResolver = Opts.option[Path]("resolver", "Iglu resolver config path")
  val selfDescribingSchema = Opts.option[SchemaKey]("schema", "Schema to check against. It should have iglu:<URI> format")
  val igluServerUrl = Opts.option[Server.HttpUrl]("server", "Iglu Server URL")
  val igluServerApiKey = Opts.option[UUID]("apikey", "Iglu Server Read ApiKey (non master)").orNone
  val dbSchema = Opts.option[String]("dbschema", "Database schema").withDefault("atomic")

  val singleTableCheck = (igluResolver, selfDescribingSchema).mapN(SingleTableCheck.apply)
  val multipleTableCheck = (igluServerUrl, igluServerApiKey).mapN(MultipleTableCheck.apply)

  // subcommands
  val staticGenerate = Opts.subcommand("generate", "Generate DDL and JSON Path files") {
    (input, output, dbschema, owner, varcharSize, withJsonPathsOpt, rawMode, splitProduct, noHeader, force).mapN(StaticGenerate.apply)
  }
  val staticDeploy = Opts.subcommand("deploy", "Master command for schema deployment")(Opts.argument[Path]("config").map(StaticDeploy))
  val staticPush = Opts.subcommand("push", "Upload Schemas from folder onto the Iglu Server") {
    (input, registryRoot, apikey, public).mapN(StaticPush.apply)
  }
  val staticPull = Opts.subcommand("pull", "Download schemas from Iglu Server to local folder") {
    (output, registryRoot, apikey.orNone).mapN(StaticPull.apply)
  }
  val staticS3Cp = Opts.subcommand("s3cp", "Upload Schemas or JSON Path files onto S3") {
    (input, bucket, s3path, accessKeyId, secretAccessKey, profile, region).mapN(StaticS3Cp.apply)
  }
  val serverKeygen = Opts.subcommand("keygen", "Generate API key on remote Iglu Server") {
    (registryRoot, apikey, vendorPrefix).mapN(ServerKeygen.apply)
  }
  val static = Opts.subcommand("static", "Work with static registry") {
    staticGenerate.orElse(staticDeploy).orElse(staticPush).orElse(staticS3Cp).orElse(staticPull)
  }
  val lint = Opts.subcommand("lint", "Validate JSON schemas") {
    (input, skipWarnings, skipChecks).mapN(Lint.apply)
  }
  val server = Opts.subcommand("server", "Communication with Iglu Server") {
    serverKeygen
  }
  val tableCheck = Opts.subcommand("table-check", "Check given schema's table structure against schema") {
    (singleTableCheck.orElse(multipleTableCheck), dbSchema, dbConfig).mapN(TableCheck.apply)
  }

  val rdbms = Opts.subcommand("rdbms", "Work with relational databases")(tableCheck)

  val igluctlCommand = Cmd(generated.ProjectSettings.name, s"Snowplow Iglu command line utils")(static.orElse(lint).orElse(server).orElse(rdbms))


  sealed trait IgluctlCommand extends Product with Serializable

  sealed trait StaticCommand extends IgluctlCommand
  case class StaticGenerate(input: Path,
                            output: Path,
                            dbSchema: String,
                            owner: Option[String],
                            varcharSize: Int,
                            withJsonPaths: Boolean,
                            rawMode: Boolean,
                            splitProduct: Boolean,
                            noHeader: Boolean,
                            force: Boolean) extends StaticCommand
  case class StaticDeploy(config: Path) extends StaticCommand
  case class StaticPush(input: Path,
                        registryRoot: Server.HttpUrl,
                        apikey: UUID,
                        public: Boolean) extends StaticCommand
  case class StaticPull(output: Path,
                        registryRoot: Server.HttpUrl,
                        apikey: Option[UUID]) extends StaticCommand
  case class StaticS3Cp(input: Path,
                        bucket: Bucket,
                        s3Path: Option[S3Path],
                        accessKeyId: Option[String],
                        secretKeyId: Option[String],
                        profile: Option[String],
                        region: Option[String]) extends StaticCommand

  case class Lint(input: Path,
                  skipWarnings: Boolean,
                  skipChecks: List[Linter]) extends IgluctlCommand

  case class ServerKeygen(server: Server.HttpUrl, masterKey: UUID, vendorPrefix: Server.VendorPrefix) extends IgluctlCommand

  case class DbConfig(host: Option[String],
                      port: Int,
                      dbname: Option[String],
                      username: Option[String],
                      password: Option[String])

  sealed trait TableCheckType extends Product with Serializable

  case class SingleTableCheck(resolver: Path,
                              schema: SchemaKey) extends TableCheckType

  case class MultipleTableCheck(igluServerUrl: Server.HttpUrl,
                                apiKey: Option[UUID]) extends TableCheckType

  case class TableCheck(tableCheckType: TableCheckType,
                        dbSchema: String,
                        storageConfig: DbConfig) extends IgluctlCommand

}

