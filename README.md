# Igluctl

![CI][ci]
![CD][cd]
[![License][license-image]][license]

Igluctl is a command-line tool that enables you to perform the most common tasks with **[Iglu][iglu]** schema registries, i.e.:

* Linting (validating) JSON Schemas
* Generating corresponding Redshift tables definition and JSON path files for JSON Schemas
* Publishing JSON Schemas stored locally to Iglu schema registries
* Publishing JSON Schemas (or any other files) stored locally to AWS S3

For complete documentation on Igluctl please refer to the [technical documentation][technical-documentation].

## Copyright and License

Igluctl is copyright 2016-2024 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[iglu]: https://docs.snowplow.io/docs/pipeline-components-and-applications/iglu/
[technical-documentation]: https://docs.snowplow.io/docs/pipeline-components-and-applications/iglu/igluctl-2/

[ci]: https://github.com/snowplow-incubator/igluctl/workflows/CI/badge.svg
[cd]: https://github.com/snowplow-incubator/igluctl/workflows/CD/badge.svg
