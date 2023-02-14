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
package commands

import java.nio.file.Paths

import cats.data._
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaVer}
import com.snowplowanalytics.iglu.ctl.File.textFile
import com.snowplowanalytics.iglu.ctl.SpecHelpers._
import org.specs2.Specification

class GenerateSpec extends Specification { def is = s2"""
  DDL-generation command (ddl) specification
    correctly convert com.amazon.aws.lambda/java_context_1 $e1
    correctly convert com.amazon.aws.lambda/java_context_2 $e2
    correctly convert com.amazon.aws.ec2/instance_identity_1 with --no-header --schema snowplow $e3
    warn about missing schema versions (addition) $e6
    warn about missing 1-0-0 schema version $e7
    correctly create migrations from 1-0-0 to 1-0-1 $e9
    correctly create migrations from 1-0-0 to 1-0-1 to 1-0-2 $e10
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
      """CREATE SCHEMA IF NOT EXISTS atomic;
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

    val output = Generate.transform( "atomic", NonEmptyList.of(input))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.amazon.aws.lambda/java_context_1.sql"), expectedDdl)), Nil, Nil)
    output must beEqualTo(expected)
  }

  def e2 = {
    val resultContent =
      """|CREATE SCHEMA IF NOT EXISTS atomic;
         |
         |CREATE TABLE IF NOT EXISTS atomic.com_amazon_aws_lambda_java_context_2 (
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
         |COMMENT ON TABLE atomic.com_amazon_aws_lambda_java_context_2 IS 'iglu:com.amazon.aws.lambda/java_context/jsonschema/2-0-0';""".stripMargin

    val input = json"""
        {
        	"$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        	"description":"Schema for an AWS Lambda Java context object, http://docs.aws.amazon.com/lambda/latest/dg/java-context-object.html",
        	"self":{
        		"vendor":"com.amazon.aws.lambda",
        		"name":"java_context",
        		"format":"jsonschema",
        		"version":"2-0-0"
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

    val output = Generate.transform("atomic", NonEmptyList.of(input))
    val expected = textFile(Paths.get("com.amazon.aws.lambda/java_context_2.sql"), resultContent)

    output.ddls.head must beEqualTo(expected)
  }

  def e3 = {
    val resultContent =
      """|CREATE SCHEMA IF NOT EXISTS snowplow;
         |
         |CREATE TABLE IF NOT EXISTS snowplow.com_amazon_aws_ec2_instance_identity_document_1 (
         |    "schema_vendor"        VARCHAR(128)   ENCODE ZSTD NOT NULL,
         |    "schema_name"          VARCHAR(128)   ENCODE ZSTD NOT NULL,
         |    "schema_format"        VARCHAR(128)   ENCODE ZSTD NOT NULL,
         |    "schema_version"       VARCHAR(128)   ENCODE ZSTD NOT NULL,
         |    "root_id"              CHAR(36)       ENCODE RAW  NOT NULL,
         |    "root_tstamp"          TIMESTAMP      ENCODE ZSTD NOT NULL,
         |    "ref_root"             VARCHAR(255)   ENCODE ZSTD NOT NULL,
         |    "ref_tree"             VARCHAR(1500)  ENCODE ZSTD NOT NULL,
         |    "ref_parent"           VARCHAR(255)   ENCODE ZSTD NOT NULL,
         |    "account_id"           VARCHAR(4096)  ENCODE ZSTD,
         |    "architecture"         VARCHAR(4096)  ENCODE ZSTD,
         |    "availability_zone"    VARCHAR(4096)  ENCODE ZSTD,
         |    "billing_products"     VARCHAR(65535) ENCODE ZSTD,
         |    "devpay_product_codes" VARCHAR(65535) ENCODE ZSTD,
         |    "image_id"             CHAR(12)       ENCODE ZSTD,
         |    "instance_id"          VARCHAR(19)    ENCODE ZSTD,
         |    "instance_type"        VARCHAR(4096)  ENCODE ZSTD,
         |    "kernel_id"            CHAR(12)       ENCODE ZSTD,
         |    "pending_time"         TIMESTAMP      ENCODE ZSTD,
         |    "private_ip"           VARCHAR(15)    ENCODE ZSTD,
         |    "ramdisk_id"           CHAR(12)       ENCODE ZSTD,
         |    "region"               VARCHAR(4096)  ENCODE ZSTD,
         |    "version"              VARCHAR(4096)  ENCODE ZSTD,
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

    val output = Generate.transform("snowplow", NonEmptyList.of(input))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.amazon.aws.ec2/instance_identity_document_1.sql"), resultContent)),
      Nil, Nil
    )

    output must beEqualTo(expected)
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
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "bar" SMALLINT ENCODE ZSTD;
        |
        |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
        |
        |END TRANSACTION;""".stripMargin

    val output = Generate.transform( "atomic", NonEmptyList.of(initial, second))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)),
      List(textFile(Paths.get("com.acme/example/1-0-0/1-0-1.sql"), expectedMigration)),
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
         |    "c_field"                 BIGINT        ENCODE ZSTD,
         |    "a_field.d_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "d_field.f_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD,
         |    "b_field"                 BIGINT        ENCODE ZSTD,
         |    "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD,
         |    "d_field.e_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "a_field.b_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "e_field.g_field"         VARCHAR(4096) ENCODE ZSTD,
         |    "f_field"                 VARCHAR(4096) ENCODE ZSTD,
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
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "c_field" BIGINT ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "a_field.d_field" VARCHAR(4096) ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "d_field.f_field" VARCHAR(4096) ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "b_field" BIGINT ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "d_field.e_field" VARCHAR(4096) ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "a_field.b_field" VARCHAR(4096) ENCODE ZSTD;
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
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "c_field" BIGINT ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.d_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "d_field.f_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "b_field" BIGINT ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "d_field.e_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "a_field.b_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "e_field.g_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "f_field" VARCHAR(4096) ENCODE ZSTD;
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
        |
        |BEGIN TRANSACTION;
        |
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "e_field.g_field" VARCHAR(4096) ENCODE ZSTD;
        |  ALTER TABLE atomic.com_acme_example_1
        |    ADD COLUMN "f_field" VARCHAR(4096) ENCODE ZSTD;
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

    val output = Generate.transform("atomic", NonEmptyList.of(initial, second, third))
    val expected = Generate.DdlOutput(
      List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)),
      List(
        textFile(Paths.get("com.acme/example/1-0-0/1-0-1.sql"), expectedMigration1),
        textFile(Paths.get("com.acme/example/1-0-0/1-1-0.sql"), expectedMigration2),
        textFile(Paths.get("com.acme/example/1-0-1/1-1-0.sql"), expectedMigration3)
      ),
      Nil
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
        |    "enum_field"          VARCHAR(9)    ENCODE TEXT255,
        |    "non_interaction_hit" BOOLEAN       ENCODE RUNLENGTH,
        |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
        |)
        |DISTSTYLE KEY
        |DISTKEY (root_id)
        |SORTKEY (root_tstamp);
        |
        |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-0';""".stripMargin

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil,  Nil)

    val output = Generate.transform("atomic", NonEmptyList.of(schema))

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
        |    "a_field.enum_field"  VARCHAR(9)    ENCODE TEXT255,
        |    "non_interaction_hit" BOOLEAN       ENCODE RUNLENGTH,
        |    FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
        |)
        |DISTSTYLE KEY
        |DISTKEY (root_id)
        |SORTKEY (root_tstamp);
        |
        |COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-0';""".stripMargin

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil, Nil)

    val output = Generate.transform( "atomic", NonEmptyList.of(schema))

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

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil, Nil)

    val output = Generate.transform( "atomic", NonEmptyList.of(schema))

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

    val expected = Generate.DdlOutput(List(textFile(Paths.get("com.acme/example_1.sql"), expectedDdl)), Nil, Nil)

    val output = Generate.transform( "atomic", NonEmptyList.of(schema))

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

    val output = Generate.transform("atomic", NonEmptyList.of(input1, input2))

    output.warnings.head must beEqualTo("Gap in revisions between iglu:com.amazon.aws.lambda/java_context/jsonschema/1-0-0 and iglu:com.amazon.aws.lambda/java_context/jsonschema/1-0-2")
  }
}
