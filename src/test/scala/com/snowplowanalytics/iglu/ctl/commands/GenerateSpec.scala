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
package commands

import java.nio.file.Paths

import cats.data._
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.ctl.File.textFile
import com.snowplowanalytics.iglu.ctl.SpecHelpers._
import com.snowplowanalytics.iglu.ctl.commands.Generate.DdlOutput
import org.specs2.Specification

class GenerateSpec extends Specification { def is = s2"""
  DDL-generation command (ddl) specification
    correctly convert com.amazon.aws.lambda/java_context_1 with default arguments $e1
    correctly convert com.amazon.aws.lambda/java_context_1 with --raw --no-header --varchar 128 $e2
    correctly convert com.amazon.aws.ec2/instance_identity_1 with --no-header --schema snowplow $e3
    correctly produce JSONPaths file for com.amazon.aws.cloudfront/wd_access_log_1 $e4
    output correct warnings for DDL-generation process $e5
    warn about missing schema versions (addition) $e6
    warn about missing 1-0-0 schema version $e7
    correctly setup table ownership $e8
    correctly create migrations from 1-0-0 to 1-0-1 $e9
    correctly create ddl for schema with enum $e11
    correctly create ddl for schema with nested enum $e12
    correctly create ddl for schema with field without type $e13
    correctly create ddl for empty schema $e14
    warn about missing schema in the transformSnowplow $e15
  """

  def e1 = {
    val input = json"""
        {
        	"$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        	"description":"Schema for an AWS Lambda Java context object, http://docs.aws.amazon.com/lambda/latest/dg/java-context-object.html",
        	"self":{
        		"vendor":"com.amazon.aws.lambda",
        		"name":"java_context",
        		"version":"1-0-0",
        		"format":"jsonschema"
        	},
        	"type":"object",
        	"properties":{
        		"functionName":{
        			"type":"string"
        		},
        		"logStreamName":{
        			"type":"string"
        		},
        		"awsRequestId":{
        			"type":"string"
        		},
        		"remainingTimeMillis":{
        			"type":"integer",
        			"minimum":0
        		},
        		"logGroupName":{
        			"type":"string"
        		},
        		"memoryLimitInMB":{
        			"type":"integer",
        			"minimum":0
        		},
        		"clientContext":{
        			"type":"object",
        			"properties":{
        				"client":{
        					"type":"object",
        					"properties":{
        						"appTitle":{
        							"type":"string"
        						},
        						"appVersionName":{
        							"type":"string"
        						},
        						"appVersionCode":{
        							"type":"string"
        						},
        						"appPackageName":{
        							"type":"string"
        						}
        					},
        					"additionalProperties":false
        				},
        				"custom":{
        					"type":"object",
        					"patternProperties":{
        						".*":{
        							"type":"string"
        						}
        					}
        				},
        				"environment":{
        					"type":"object",
        					"patternProperties":{
        						".*":{
        							"type":"string"
        						}
        					}
        				}
        			},
        			"additionalProperties":false
        		},
        		"identity":{
        			"type":"object",
        			"properties":{
        				"identityId":{
        					"type":"string"
        				},
        				"identityPoolId":{
        					"type":"string"
        				}
        			},
        			"additionalProperties":false
        		}
        	},
        	"additionalProperties":false
        }
      """.schema

    val expectedDdl =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
         |
         |CREATE TABLE IF NOT EXISTS atomic.com_amazon_aws_lambda_java_context_1 (
         |    "schema_vendor"                          VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_name"                            VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_format"                          VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_version"                         VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "root_id"                                CHAR(36)      ENCODE RAW  NOT NULL,
         |    "root_tstamp"                            TIMESTAMP     ENCODE ZSTD NOT NULL,
         |    "ref_root"                               VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    "ref_tree"                               VARCHAR(1500) ENCODE ZSTD NOT NULL,
         |    "ref_parent"                             VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    "aws_request_id"                         VARCHAR(4096) ENCODE ZSTD,
         |    "client_context.client.app_package_name" VARCHAR(4096) ENCODE ZSTD,
         |    "client_context.client.app_title"        VARCHAR(4096) ENCODE ZSTD,
         |    "client_context.client.app_version_code" VARCHAR(4096) ENCODE ZSTD,
         |    "client_context.client.app_version_name" VARCHAR(4096) ENCODE ZSTD,
         |    "client_context.custom"                  VARCHAR(4096) ENCODE ZSTD,
         |    "client_context.environment"             VARCHAR(4096) ENCODE ZSTD,
         |    "function_name"                          VARCHAR(4096) ENCODE ZSTD,
         |    "identity.identity_id"                   VARCHAR(4096) ENCODE ZSTD,
         |    "identity.identity_pool_id"              VARCHAR(4096) ENCODE ZSTD,
         |    "log_group_name"                         VARCHAR(4096) ENCODE ZSTD,
         |    "log_stream_name"                        VARCHAR(4096) ENCODE ZSTD,
         |    "memory_limit_in_mb"                     BIGINT        ENCODE ZSTD,
         |    "remaining_time_millis"                  BIGINT        ENCODE ZSTD,
         |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
         |)
         |DISTSTYLE KEY
         |DISTKEY (root_id)
         |SORTKEY (root_tstamp);
         |
         |COMMENT ON TABLE atomic.com_amazon_aws_lambda_java_context_1 IS 'iglu:com.amazon.aws.lambda/java_context/jsonschema/1-0-0';""".stripMargin

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(input))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.amazon.aws.lambda/java_context_1.sql"), expectedDdl)),
      Nil, Nil, Nil
    )
    output must beEqualTo(expected)
  }

  def e2 = {
    val resultContent =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
         |
         |CREATE TABLE IF NOT EXISTS atomic.com_amazon_aws_lambda_java_context_1 (
         |    "aws_request_id"                         VARCHAR(128) ENCODE ZSTD,
         |    "client_context.client.app_package_name" VARCHAR(128) ENCODE ZSTD,
         |    "client_context.client.app_title"        VARCHAR(128) ENCODE ZSTD,
         |    "client_context.client.app_version_code" VARCHAR(128) ENCODE ZSTD,
         |    "client_context.client.app_version_name" VARCHAR(128) ENCODE ZSTD,
         |    "client_context.custom"                  VARCHAR(128) ENCODE ZSTD,
         |    "client_context.environment"             VARCHAR(128) ENCODE ZSTD,
         |    "function_name"                          VARCHAR(128) ENCODE ZSTD,
         |    "identity.identity_id"                   VARCHAR(128) ENCODE ZSTD,
         |    "identity.identity_pool_id"              VARCHAR(128) ENCODE ZSTD,
         |    "log_group_name"                         VARCHAR(128) ENCODE ZSTD,
         |    "log_stream_name"                        VARCHAR(128) ENCODE ZSTD,
         |    "memory_limit_in_mb"                     BIGINT       ENCODE ZSTD,
         |    "remaining_time_millis"                  BIGINT       ENCODE ZSTD
         |);
         |
         |COMMENT ON TABLE atomic.com_amazon_aws_lambda_java_context_1 IS 'iglu:com.amazon.aws.lambda/java_context/jsonschema/1-0-0';""".stripMargin

    val input = json"""
        {
        	"description":"Schema for an AWS Lambda Java context object, http://docs.aws.amazon.com/lambda/latest/dg/java-context-object.html",
        	"self":{
        		"vendor":"com.amazon.aws.lambda",
        		"name":"java_context",
        		"format":"jsonschema",
        		"version":"1-0-0"
         },
        	"type":"object",
        	"properties":{
        		"functionName":{
        			"type":"string"
        		},
        		"logStreamName":{
        			"type":"string"
        		},
        		"awsRequestId":{
        			"type":"string"
        		},
        		"remainingTimeMillis":{
        			"type":"integer",
        			"minimum":0
        		},
        		"logGroupName":{
        			"type":"string"
        		},
        		"memoryLimitInMB":{
        			"type":"integer",
        			"minimum":0
        		},
        		"clientContext":{
        			"type":"object",
        			"properties":{
        				"client":{
        					"type":"object",
        					"properties":{
        						"appTitle":{
        							"type":"string"
        						},
        						"appVersionName":{
        							"type":"string"
        						},
        						"appVersionCode":{
        							"type":"string"
        						},
        						"appPackageName":{
        							"type":"string"
        						}
        					},
        					"additionalProperties":false
        				},
        				"custom":{
        					"type":"object",
        					"patternProperties":{
        						".*":{
        							"type":"string"
        						}
        					}
        				},
        				"environment":{
        					"type":"object",
        					"patternProperties":{
        						".*":{
        							"type":"string"
        						}
        					}
        				}
        			},
        			"additionalProperties":false
        		},
        		"identity":{
        			"type":"object",
        			"properties":{
        				"identityId":{
        					"type":"string"
        				},
        				"identityPoolId":{
        					"type":"string"
        				}
        			},
        			"additionalProperties":false
        		}
        	},
        	"additionalProperties":false
        }
      """.schema

    val output = Generate.transform(false, "atomic", 128, false, true, None, true)(NonEmptyList.of(input))
    val expected = List(textFile(Paths.get("com.amazon.aws.lambda/java_context_1.sql"), resultContent))

    output.ddls must beEqualTo(expected)
  }

  def e3 = {
    val resultContent =
      """|CREATE SCHEMA IF NOT EXISTS snowplow;
         |
         |CREATE TABLE IF NOT EXISTS snowplow.com_amazon_aws_ec2_instance_identity_document_1 (
         |    "schema_vendor"        VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_name"          VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_format"        VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_version"       VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "root_id"              CHAR(36)      ENCODE RAW  NOT NULL,
         |    "root_tstamp"          TIMESTAMP     ENCODE ZSTD NOT NULL,
         |    "ref_root"             VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    "ref_tree"             VARCHAR(1500) ENCODE ZSTD NOT NULL,
         |    "ref_parent"           VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    "account_id"           VARCHAR(4096) ENCODE ZSTD,
         |    "architecture"         VARCHAR(4096) ENCODE ZSTD,
         |    "availability_zone"    VARCHAR(4096) ENCODE ZSTD,
         |    "billing_products"     VARCHAR(5000) ENCODE ZSTD,
         |    "devpay_product_codes" VARCHAR(5000) ENCODE ZSTD,
         |    "image_id"             CHAR(12)      ENCODE ZSTD,
         |    "instance_id"          VARCHAR(19)   ENCODE ZSTD,
         |    "instance_type"        VARCHAR(4096) ENCODE ZSTD,
         |    "kernel_id"            CHAR(12)      ENCODE ZSTD,
         |    "pending_time"         TIMESTAMP     ENCODE ZSTD,
         |    "private_ip"           VARCHAR(15)   ENCODE ZSTD,
         |    "ramdisk_id"           CHAR(12)      ENCODE ZSTD,
         |    "region"               VARCHAR(4096) ENCODE ZSTD,
         |    "version"              VARCHAR(4096) ENCODE ZSTD,
         |    FOREIGN KEY (root_id) REFERENCES snowplow.events(event_id)
         |)
         |DISTSTYLE KEY
         |DISTKEY (root_id)
         |SORTKEY (root_tstamp);
         |
         |COMMENT ON TABLE snowplow.com_amazon_aws_ec2_instance_identity_document_1 IS 'iglu:com.amazon.aws.ec2/instance_identity_document/jsonschema/1-0-0';""".stripMargin

    val input = json"""
         {
           "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
           "self" : {
             "vendor" : "com.amazon.aws.ec2",
             "name" : "instance_identity_document",
             "version" : "1-0-0",
             "format" : "jsonschema"
           },
           "type" : "object",
           "properties" : {
             "instanceId" : {
               "type" : "string",
               "minLength" : 10,
               "maxLength" : 19
             },
             "devpayProductCodes" : {
               "type" : [ "array", "null" ],
               "items" : {
                 "type" : "string"
               }
             },
             "billingProducts" : {
               "type" : [ "array", "null" ],
               "items" : {
                 "type" : "string"
               }
             },
             "availabilityZone" : {
               "type" : "string"
             },
             "accountId" : {
               "type" : "string"
             },
             "ramdiskId" : {
               "type" : [ "string", "null" ],
               "minLength" : 12,
               "maxLength" : 12
             },
             "architecture" : {
               "type" : "string"
             },
             "instanceType" : {
               "type" : "string"
             },
             "version" : {
               "type" : "string"
             },
             "pendingTime" : {
               "type" : "string",
               "format" : "date-time"
             },
             "imageId" : {
               "type" : "string",
               "minLength" : 12,
               "maxLength" : 12
             },
             "privateIp" : {
               "type" : "string",
               "format" : "ipv4",
               "minLength" : 11,
               "maxLength" : 15
             },
             "region" : {
               "type" : "string"
             },
             "kernelId" : {
               "type" : [ "string", "null" ],
               "minLength" : 12,
               "maxLength" : 12
             }
           },
           "additionalProperties" : false
         }""".schema

    val output = Generate.transform(false, "snowplow", 4096, false, true, None, false)(NonEmptyList.of(input))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.amazon.aws.ec2/instance_identity_document_1.sql"), resultContent)),
      Nil, Nil, Nil
    )

    output must beEqualTo(expected)
  }

  def e4 = {
    val input = json"""
         {
         	"$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
         	"description": "Schema for a AWS CloudFront web distribution access log event. Version released 01 Jul 2014",
         	"self": {
         		"vendor": "com.amazon.aws.cloudfront",
         		"name": "wd_access_log",
         		"format": "jsonschema",
         		"version": "1-0-4"
         	},

         	"type": "object",
         	"properties": {
         		"dateTime": {
         			"type": "string",
         			"format": "date-time"
         		},
         		"xEdgeLocation": {
         			"type": ["string", "null"],
         			"maxLength": 32
         		},
         		"scBytes": {
         			"type": ["number", "null"]
         		},
         		"cIp": {
         			"type": ["string", "null"],
         			"maxLength": 45
         		},
         		"csMethod": {
         			"type": ["string", "null"],
         			"maxLength": 3
         		},
         		"csHost": {
         			"type": ["string", "null"],
         			"maxLength": 2000
         		},
         		"csUriStem": {
         			"type": ["string", "null"],
         			"maxLength": 8192
         		},
         		"scStatus": {
         			"type": ["string", "null"],
         			"maxLength": 3
         		},
         		"csReferer": {
         			"type": ["string", "null"],
         			"maxLength": 8192
         		},
         		"csUserAgent": {
         			"type": ["string", "null"],
         			"maxLength": 2000
         		},
         		"csUriQuery": {
         			"type": ["string", "null"],
         			"maxLength": 8192
         		},
         		"csCookie": {
         			"type": ["string", "null"],
         			"maxLength": 4096
         		},
         		"xEdgeResultType": {
         			"type": ["string", "null"],
         			"maxLength": 32
         		},
         		"xEdgeRequestId": {
         			"type": ["string", "null"],
         			"maxLength": 2000
         		},
         		"xHostHeader": {
         			"type": ["string", "null"],
         			"maxLength": 2000
         		},
         		"csProtocol": {
         			"enum": ["http", "https", null]
         		},
         		"csBytes": {
         			"type": ["number", "null"]
         		},
         		"timeTaken": {
         			"type": ["number", "null"]
         		},
         		"xForwardedFor": {
         			"type": ["string", "null"],
         			"maxLength": 45
         		},
         		"sslProtocol": {
         			"type": ["string", "null"],
         			"maxLength": 32
         		},
         		"sslCipher": {
         			"type": ["string", "null"],
         			"maxLength": 64
         		},
         		"xEdgeResponseResultType": {
         			"type": ["string", "null"],
         			"maxLength": 32
         		}
         	},
         	"required": ["dateTime"],
         	"additionalProperties": false
         }""".schema

    val resultContent =
      """|{
         |    "jsonpaths": [
         |        "$.schema.vendor",
         |        "$.schema.name",
         |        "$.schema.format",
         |        "$.schema.version",
         |        "$.hierarchy.rootId",
         |        "$.hierarchy.rootTstamp",
         |        "$.hierarchy.refRoot",
         |        "$.hierarchy.refTree",
         |        "$.hierarchy.refParent",
         |        "$.data.dateTime",
         |        "$.data.cIp",
         |        "$.data.csBytes",
         |        "$.data.csCookie",
         |        "$.data.csHost",
         |        "$.data.csMethod",
         |        "$.data.csProtocol",
         |        "$.data.csReferer",
         |        "$.data.csUriQuery",
         |        "$.data.csUriStem",
         |        "$.data.csUserAgent",
         |        "$.data.scBytes",
         |        "$.data.scStatus",
         |        "$.data.sslCipher",
         |        "$.data.sslProtocol",
         |        "$.data.timeTaken",
         |        "$.data.xEdgeLocation",
         |        "$.data.xEdgeRequestId",
         |        "$.data.xEdgeResponseResultType",
         |        "$.data.xEdgeResultType",
         |        "$.data.xForwardedFor",
         |        "$.data.xHostHeader"
         |    ]
         |}""".stripMargin

    val output = Generate.transform(true, "atomic", 4096, false, false, None, false)(NonEmptyList.of(input)).jsonPaths.head

    val expected = textFile(Paths.get("com.amazon.aws.cloudfront/wd_access_log_1.json"), resultContent)

    output must beEqualTo(expected)
  }

  def e5 = {
    import com.snowplowanalytics.iglu.ctl.commands.Generate._
    import com.snowplowanalytics.iglu.schemaddl.redshift._
    import com.snowplowanalytics.iglu.schemaddl.redshift.generators._

    val definitions = List(
      TableDefinition("com.acme", "some_event", DdlFile(  // Definition with 2 product-type columns
        List(
          CommentBlock(Vector("AUTO-GENERATED BY schema-ddl DO NOT EDIT", "Generator: schema-ddl 0.2.0", "Generated: 2016-03-31 15:52")),
          CreateTable("some_event", List(
            Column("action", ProductType(List("warning1", "warning2"), None)),
            Column("time_stamp", RedshiftBigInt),
            Column("subject", ProductType(List("warning3"), None))
          )),
          CommentOn("some_event", "iglu:com.acme/some_event/jsonschema/1-0-0")
        )
      ), List()),

      TableDefinition("com.acme", "some_context", DdlFile(  // Definition without warnings
        List(
          CommentBlock(Vector("AUTO-GENERATED BY schema-ddl DO NOT EDIT", "Generator: schema-ddl 0.2.0", "Generated: 2016-03-31 15:59")),
          CreateTable("some_context", List(
            Column("time_stamp", RedshiftBigInt)
          )),
          CommentOn("some_context", "iglu:com.acme/some_context/jsonschema/1-0-1")
        )
      ), List()),

      TableDefinition("com.acme", "other_context_1", DdlFile( // Definition without iglu URI (impossible though)
        List(
          CommentBlock(Vector("AUTO-GENERATED BY schema-ddl DO NOT EDIT", "Generator: schema-ddl 0.2.0", "Generated: 2016-03-31 15:59")),
          CreateTable("other_context", List(
            Column("values", ProductType(List("another_warning"), None))
          ))
        )
      ), List())
    )

    getDdlWarnings(definitions) must beEqualTo(List(
      "Warning: in JSON Schema [iglu:com.acme/some_event/jsonschema/1-0-0]: warning1",
      "Warning: in JSON Schema [iglu:com.acme/some_event/jsonschema/1-0-0]: warning2",
      "Warning: in JSON Schema [iglu:com.acme/some_event/jsonschema/1-0-0]: warning3",
      "Warning: in generated DDL [com.acme/other_context_1]: another_warning"
    ))
  }

  def e6 = {
    val input = NonEmptyList.of(
      SchemaMap("com.example-agency","cast","jsonschema",SchemaVer.Full(1,0,0)),
      SchemaMap("com.example-agency","cast","jsonschema",SchemaVer.Full(1,0,3)))
    val output = Common.checkSchemasConsistency(input)

    output must beLeft(NonEmptyList.of(Common.GapError.Gaps("com.example-agency", "cast")))
  }

  def e7 = {
    val schemaMaps = NonEmptyList.of(
      SchemaMap("com.example-agency", "cast", "jsonschema", SchemaVer.Full(1,1,0)),
      SchemaMap("com.example-agency", "cast", "jsonschema", SchemaVer.Full(1,1,1)))

    val output = Common.checkSchemasConsistency(schemaMaps)

    output must beLeft(NonEmptyList.of(Common.GapError.InitMissing("com.example-agency", "cast")))
  }

  def e8 = {
    val input = json"""
        {
              "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
              "description": "Schema for custom contexts",
        	     "self": {
        	     	 "vendor": "com.acme.persons",
        	     	 "name": "simple",
        	     	 "format": "jsonschema",
        	     	 "version": "1-0-0"
        	     },

              "type": "object",
              "properties": {
                "name": { "type": "string" },
                "age": { "type": "number" }
              },
              "required":["name"]
        }
      """.schema

    val resultContent =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
         |
         |CREATE TABLE IF NOT EXISTS atomic.com_acme_persons_simple_1 (
         |    "name" VARCHAR(4096)    ENCODE ZSTD NOT NULL,
         |    "age"  DOUBLE PRECISION ENCODE RAW
         |);
         |
         |COMMENT ON TABLE atomic.com_acme_persons_simple_1 IS 'iglu:com.acme.persons/simple/jsonschema/1-0-0';
         |
         |ALTER TABLE atomic.com_acme_persons_simple_1 OWNER TO storageloader;""".stripMargin

    val output = Generate.transform(false, "atomic", 4096, false, true, Some("storageloader"), true)(NonEmptyList.of(input))

    val expected = List(textFile(Paths.get("com.acme.persons/simple_1.sql"), resultContent))

    output.ddls must beEqualTo(expected)
  }

  def e9 = {
    val initial = json"""
        {
          "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self":{
            "vendor":"com.acme",
            "name":"example",
            "version":"1-0-0",
            "format":"jsonschema"
          },
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            }
          },
          "additionalProperties": false
        }
      """.schema

    val second = json"""
        {
          "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self":{
            "vendor":"com.acme",
            "name":"example",
            "version":"1-0-1",
            "format":"jsonschema"
          },
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema

    val expectedDdl =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
        |
        |CREATE TABLE IF NOT EXISTS atomic.com_acme_example_1 (
        |    "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
        |    "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
        |    "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
        |    "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
        |    "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
        |    "foo"            VARCHAR(4096) ENCODE ZSTD,
        |    "bar"            SMALLINT      ENCODE ZSTD,
        |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
        |)
        |DISTSTYLE KEY
        |DISTKEY (root_id)
        |SORTKEY (root_tstamp);
        |
        |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';""".stripMargin

    val expectedMigration =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
        |--
        |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
        |--  obj_description
        |-- -----------------
        |--  iglu:com.acme/example/jsonschema/1-0-0
        |--  (1 row)
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "bar" SMALLINT ENCODE ZSTD;
        |
        |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
        |
        |END TRANSACTION;""".stripMargin

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(initial, second))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)),
      List(textFile(Paths.get("com.acme/example/1-0-0/1-0-1.sql"), expectedMigration)),
      Nil,
      Nil
    )

    output must beEqualTo(expected)
  }

  def e10 = {
    val initial = json"""
        {
          "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self":{
            "vendor":"com.acme",
            "name":"example",
            "version":"1-0-0",
            "format":"jsonschema"
          },
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema

    val second = json"""
        {
          "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self":{
            "vendor":"com.acme",
            "name":"example",
            "version":"1-0-1",
            "format":"jsonschema"
          },
          "type": "object",
          "properties": {
           "foo": {
             "type": "string",
             "maxLength": 20
           },
           "bar": {
             "type": "integer",
             "maximum": 4000
           },
           "a_field": {
             "type": "object",
             "properties": {
               "b_field": {
                 "type": "string"
               },
               "c_field": {
                 "type": "object",
                 "properties": {
                   "d_field": {
                     "type": "string"
                   },
                   "e_field": {
                     "type": "string"
                   }
                 }
               },
               "d_field": {
                 "type": "object"
               }
             },
             "required": ["d_field"]
           },
           "b_field": {
             "type": "integer"
           },
           "c_field": {
             "type": "integer"
           },
           "d_field": {
             "type": "object",
             "properties": {
               "e_field": {
                 "type": "string"
               },
               "f_field": {
                 "type": "string"
               }
             }
           }
          },
          "required": ["a_field"],
          "additionalProperties": false
        }
      """.schema

    val third = json"""
        {
          "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self":{
            "vendor":"com.acme",
            "name":"example",
            "version":"1-1-0",
            "format":"jsonschema"
          },
          "type": "object",
          "properties": {
            "foo": {
              "type": "string",
              "maxLength": 20
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            },
            "a_field": {
              "type": "object",
              "properties": {
                "b_field": {
                  "type": "string"
                },
                "c_field": {
                  "type": "object",
                  "properties": {
                    "e_field": {
                      "type": "string"
                    },
                    "d_field": {
                      "type": "string"
                    },
                    "a_field": {
                      "type": "string"
                    }
                  }
                },
                "d_field": {
                  "type": "object"
                }
              },
              "required": ["d_field"]
            },
            "d_field": {
              "type": "object",
              "properties": {
                "f_field": {
                  "type": "string"
                }
              }
            },
            "b_field": {
              "type": "integer"
            },
            "c_field": {
              "type": "integer"
            },
            "e_field": {
              "type": "object",
              "properties": {
                "f_field": {
                  "type": "string"
                },
                "g_field": {
                  "type": "string"
                }
              },
              "required": ["g_field"]
            },
            "f_field": {
              "type": "string"
            },
            "g_field": {
              "type": "string"
            }
          },
          "required": ["a_field", "f_field", "e_field"],
          "additionalProperties": false
        }
      """.schema

    val expectedDdl =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
        |
        |CREATE TABLE IF NOT EXISTS atomic.com_acme_example_1 (
         |    "schema_vendor"           VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_name"             VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_format"           VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_version"          VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "root_id"                 CHAR(36)      ENCODE RAW  NOT NULL,
         |    "root_tstamp"             TIMESTAMP     ENCODE ZSTD NOT NULL,
         |    "ref_root"                VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    "ref_tree"                VARCHAR(1500) ENCODE ZSTD NOT NULL,
         |    "ref_parent"              VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    "bar"                     SMALLINT      ENCODE ZSTD,
         |    "foo"                     VARCHAR(4096) ENCODE ZSTD,
         |    "a_field.d_field"         VARCHAR(4096) ENCODE ZSTD NOT NULL,
         |    "a_field.b_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD,
         |    "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD,
         |    "b_field"                 BIGINT        ENCODE ZSTD,
         |    "c_field"                 BIGINT        ENCODE ZSTD,
         |    "d_field.e_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "d_field.f_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "e_field.g_field"         VARCHAR(4096) ENCODE ZSTD NOT NULL,
         |    "f_field"                 VARCHAR(4096) ENCODE ZSTD NOT NULL,
         |    "a_field.c_field.a_field" VARCHAR(4096) ENCODE ZSTD,
         |    "e_field.f_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "g_field"                 VARCHAR(4096) ENCODE ZSTD,
        |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
        |)
        |DISTSTYLE KEY
        |DISTKEY (root_id)
        |SORTKEY (root_tstamp);
        |
        |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-1-0';""".stripMargin

    val expectedMigration1 =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
        |--
        |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
        |--  obj_description
        |-- -----------------
        |--  iglu:com.acme/example/jsonschema/1-0-0
        |--  (1 row)
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.d_field" VARCHAR(4096) NOT NULL ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.b_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "b_field" BIGINT ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "c_field" BIGINT ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "d_field.e_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "d_field.f_field" VARCHAR(4096) ENCODE ZSTD;
        |
        |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
        |
        |END TRANSACTION;""".stripMargin

    val expectedMigration2 =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
        |--
        |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
        |--  obj_description
        |-- -----------------
        |--  iglu:com.acme/example/jsonschema/1-0-0
        |--  (1 row)
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.d_field" VARCHAR(4096) NOT NULL ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.b_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "b_field" BIGINT ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "c_field" BIGINT ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "d_field.e_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "d_field.f_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "e_field.g_field" VARCHAR(4096) NOT NULL ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "f_field" VARCHAR(4096) NOT NULL ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.a_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "e_field.f_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "g_field" VARCHAR(4096) ENCODE ZSTD;
        |
        |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-1-0';
        |
        |END TRANSACTION;""".stripMargin

    val expectedMigration3 =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
        |--
        |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
        |--  obj_description
        |-- -----------------
        |--  iglu:com.acme/example/jsonschema/1-0-1
        |--  (1 row)
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "e_field.g_field" VARCHAR(4096) NOT NULL ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "f_field" VARCHAR(4096) NOT NULL ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.a_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "e_field.f_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "g_field" VARCHAR(4096) ENCODE ZSTD;
        |
        |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-1-0';
        |
        |END TRANSACTION;""".stripMargin

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(initial, second, third))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)),
      List(
        textFile(Paths.get("com.acme/example/1-0-1/1-1-0.sql"), expectedMigration3),
        textFile(Paths.get("com.acme/example/1-0-0/1-0-1.sql"), expectedMigration1),
        textFile(Paths.get("com.acme/example/1-0-0/1-1-0.sql"), expectedMigration2)
      ),
      Nil,
      List(
        "Ambiguous order in the following schemas, NonEmptyList(SchemaKey(com.acme,example,jsonschema,Full(1,0,0)), SchemaKey(com.acme,example,jsonschema,Full(1,0,1)), SchemaKey(com.acme,example,jsonschema,Full(1,1,0)))"
      )
    )

    output must beEqualTo(expected)
  }

  def e11 = {
    val schema = json"""
        {
         "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
         "self":{
           "vendor":"com.acme",
           "name":"example",
           "version":"1-0-0",
           "format":"jsonschema"
         },
         "type": "object",
         "properties": {
           "enum_field": {
             "enum": [
               "event",
               "exception",
               "item"
             ]
           },
           "nonInteractionHit": {
             "type": ["boolean", "null"]
           }
         },
         "additionalProperties": false
        }
      """.schema

    val expectedDdl =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
        |
        |CREATE TABLE IF NOT EXISTS atomic.com_acme_example_1 (
        |    "schema_vendor"       VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "schema_name"         VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "schema_format"       VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "schema_version"      VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "root_id"             CHAR(36)      ENCODE RAW       NOT NULL,
        |    "root_tstamp"         TIMESTAMP     ENCODE ZSTD      NOT NULL,
        |    "ref_root"            VARCHAR(255)  ENCODE ZSTD      NOT NULL,
        |    "ref_tree"            VARCHAR(1500) ENCODE ZSTD      NOT NULL,
        |    "ref_parent"          VARCHAR(255)  ENCODE ZSTD      NOT NULL,
        |    "enum_field"          VARCHAR(9)    ENCODE ZSTD,
        |    "non_interaction_hit" BOOLEAN       ENCODE RUNLENGTH,
        |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
        |)
        |DISTSTYLE KEY
        |DISTKEY (root_id)
        |SORTKEY (root_tstamp);
        |
        |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-0';""".stripMargin

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil, Nil, Nil)

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(schema))

    output must beEqualTo(expected)
  }

  def e12 = {
    val schema = json"""
        {
         "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
         "self":{
           "vendor":"com.acme",
           "name":"example",
           "version":"1-0-0",
           "format":"jsonschema"
         },
         "type": "object",
         "properties": {
           "a_field": {
            "type": "object",
            "properties": {
             "enum_field": {
               "enum": [
                 "event",
                 "exception",
                 "item"
               ]
             }
            }
           },
           "nonInteractionHit": {
             "type": ["boolean", "null"]
           }
         },
         "additionalProperties": false
        }
      """.schema

    val expectedDdl =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
        |
        |CREATE TABLE IF NOT EXISTS atomic.com_acme_example_1 (
        |    "schema_vendor"       VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "schema_name"         VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "schema_format"       VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "schema_version"      VARCHAR(128)  ENCODE ZSTD      NOT NULL,
        |    "root_id"             CHAR(36)      ENCODE RAW       NOT NULL,
        |    "root_tstamp"         TIMESTAMP     ENCODE ZSTD      NOT NULL,
        |    "ref_root"            VARCHAR(255)  ENCODE ZSTD      NOT NULL,
        |    "ref_tree"            VARCHAR(1500) ENCODE ZSTD      NOT NULL,
        |    "ref_parent"          VARCHAR(255)  ENCODE ZSTD      NOT NULL,
        |    "a_field.enum_field"  VARCHAR(9)    ENCODE ZSTD,
        |    "non_interaction_hit" BOOLEAN       ENCODE RUNLENGTH,
        |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
        |)
        |DISTSTYLE KEY
        |DISTKEY (root_id)
        |SORTKEY (root_tstamp);
        |
        |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-0';""".stripMargin

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil, Nil, Nil)

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(schema))

    output must beEqualTo(expected)
  }

  def e13 = {
    val schema = json"""
        {
          "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self":{
            "vendor":"com.acme",
            "name":"example",
            "version":"1-0-0",
            "format":"jsonschema"
          },
          "type": "object",
          "properties": {
            "a_field": { "type": "string" },
            "b_field": {}
          }
        }
      """.schema

    val expectedDdl =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
        |
        |CREATE TABLE IF NOT EXISTS atomic.com_acme_example_1 (
        |    "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
        |    "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
        |    "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
        |    "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
        |    "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
        |    "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
        |    "a_field"        VARCHAR(4096) ENCODE ZSTD,
        |    "b_field"        VARCHAR(4096) ENCODE ZSTD,
        |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
        |)
        |DISTSTYLE KEY
        |DISTKEY (root_id)
        |SORTKEY (root_tstamp);
        |
        |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-0';""".stripMargin

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil, Nil, Nil)

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(schema))

    output must beEqualTo(expected)
  }

  def e14 = {
    val schema = json"""
        {
          "$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
          "self":{
            "vendor":"com.acme",
            "name":"example",
            "version":"1-0-0",
            "format":"jsonschema"
          },
          "type": "object",
          "additionalProperties": false
        }
      """.schema

    val expectedDdl =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
         |
         |CREATE TABLE IF NOT EXISTS atomic.com_acme_example_1 (
         |    "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
         |    "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
         |    "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
         |    "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
         |    "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
         |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
         |)
         |DISTSTYLE KEY
         |DISTKEY (root_id)
         |SORTKEY (root_tstamp);
         |
         |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-0';""".stripMargin

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil, Nil, Nil)

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(schema))

    output must beEqualTo(expected)
  }

  def e15 = {
    val input1 = json"""
        {
        	"$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        	"description":"Schema for an AWS Lambda Java context object, http://docs.aws.amazon.com/lambda/latest/dg/java-context-object.html",
        	"self":{
        		"vendor":"com.amazon.aws.lambda",
        		"name":"java_context",
        		"version":"1-0-0",
        		"format":"jsonschema"
        	},
        	"type":"object",
        	"properties":{
        		"functionName":{
        			"type":"string"
        		},
        		"logStreamName":{
        			"type":"string"
        		},
        		"awsRequestId":{
        			"type":"string"
        		}
        	},
        	"additionalProperties":false
        }
      """.schema

    val input2 = json"""
        {
        	"$$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        	"description":"Schema for an AWS Lambda Java context object, http://docs.aws.amazon.com/lambda/latest/dg/java-context-object.html",
        	"self":{
        		"vendor":"com.amazon.aws.lambda",
        		"name":"java_context",
        		"version":"1-0-2",
        		"format":"jsonschema"
        	},
        	"type":"object",
        	"properties":{
        		"functionName":{
        			"type":"string"
        		},
        		"logStreamName":{
        			"type":"string"
        		},
        		"awsRequestId":{
        			"type":"string"
        		}
        	},
        	"additionalProperties":false
        }
      """.schema

    val output = Generate.transform(false, "atomic", 4096, false, true, None, false)(NonEmptyList.of(input1, input2))

    val expectedWarning = "Gap in the following model group schemas, NonEmptyList(SchemaKey(com.amazon.aws.lambda,java_context,jsonschema,Full(1,0,0)), SchemaKey(com.amazon.aws.lambda,java_context,jsonschema,Full(1,0,2)))"

    val expected = DdlOutput(Nil, Nil, Nil, List(expectedWarning))

    output must beEqualTo(expected)
  }
}
