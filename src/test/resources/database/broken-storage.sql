CREATE SCHEMA IF NOT EXISTS atomic;

CREATE TABLE IF NOT EXISTS atomic.com_test_test_1 (
    "schema_vendor"     VARCHAR(128)    NOT NULL,
    "schema_name"       VARCHAR(128)    NOT NULL,
    "schema_format"     VARCHAR(128)    NOT NULL,
    "schema_version"    VARCHAR(128)    NOT NULL,
    "root_id"           CHAR(36)        NOT NULL,
    "root_tstamp"       TIMESTAMP       NOT NULL,
    "ref_root"          VARCHAR(255)    NOT NULL,
    "ref_tree"          VARCHAR(1500)   NOT NULL,
    "ref_parent"        VARCHAR(255)    NOT NULL,
    
--  custom columns:
    "only_in_storage"   VARCHAR(4096),
    "wrong_type"        VARCHAR(4096),
    "wrong_nullability" VARCHAR(4096)   NOT NULL
);

COMMENT ON TABLE atomic.com_test_test_1 IS 'iglu:com.another/test/jsonschema/1-0-0';
