# Igluctl

![CI][ci]
![CD][cd]
[![License][license-image]][license]

Igluctl is command-line tool, that enables you to perform the most common tasks with **[Iglu][iglu]** schema registries, i.e.:

* Linting (validating) JSON Schemas
* Generating corresponding Redshift tables definition and JSON path files for JSON Schemas
* Publish JSON Schemas stored locally to Iglu schema registries
* Publish JSON Schemas (or any other files) stored locally to AWS S3

For complete documenation on Igluctl please refer to the [technical documentation][technical-documentation].

## User Quickstart

Make sure you have [Oracle JRE 8][jre] installed.

Artifacts are attached to GitHub releases. e.g. 0.10.0 can be downloaded from

```
https://github.com/snowplow-incubator/igluctl/releases/download/0.10.0/igluctl
```

### Windows

`igluctl` is a single executable file.

To run it you can use following format:

```bash
$ java -jar igluctl lint $SCHEMAS_DIR
```

Below and everywhere in documentation you'll find example commands without `java -jar` prefix, which is Windows-specific.

### Mac OS X and Linux

You can run igluctl using following commands:

```bash
$ ./igluctl lint $SCHEMAS_DIR
```

## CLI

### Generate DDL

You can transform JSON Schema into Redshift (other storages are coming) DDL, using `igluctl static generate` command.

```bash
$ ./igluctl static generate $SCHEMAS_DIR
```

`$SCHEMAS_DIR` should be the path to the JSON Schemas stored locally that are to be validated.

### Publish JSON Schemas to a remote Iglu Schema Registry

You can publish your JSON Schemas from local filesystem to Iglu Scala Registry using `igluctl static push` command.


```bash
$ ./igluctl static push $SCHEMAS_DIR $IGLU_HOST $APIKEY
```

If you are running an s3 backed Iglu Static Server Registry you can publish schemas as follows:

```
$ ./igluctl static s3cp $SCHEMAS_DIR $S3BUCKET --accessKeyId $ACCESS_KEY_ID --secretAccessKey $SECRET_ACCESS_KEY --region $AWS_REGION
```

Igluctl will closely follow [AWS CLI][aws-cli] tools behavior while looking for credentials, which means you can omit `accessKeyId` and `secretKeyId` options
if you have AWS `default` profile or appropriate environment variables.

### Linting

You can check your JSON Schema for vairous common mistakes using `igluctl lint` command.

```bash
$ ./igluctl lint $SCHEMAS_DIR
```

This check will include JSON Syntax validation (`required` is not empty, `maximum` is integer etc)
and also "sanity check", which checks that particular JSON Schema can always validate at least one possible JSON.


## Copyright and License

Igluctl is copyright 2016-2022 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[iglu]: https://github.com/snowplow/iglu
[schema-guru]: https://github.com/snowplow/schema-guru
[technical-documentation]: https://github.com/snowplow/iglu/wiki/Igluctl

[jre]: http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html
[aws-cli]: http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#config-settings-and-precedence

[ci]: https://github.com/snowplow-incubator/igluctl/workflows/CI/badge.svg
[cd]: https://github.com/snowplow-incubator/igluctl/workflows/CD/badge.svg
