CREATE SCHEMA IF NOT EXISTS atomic;

CREATE TABLE IF NOT EXISTS atomic.com_test_test_1 (
    "schema_vendor"    VARCHAR(128)    NOT NULL,
    "schema_name"      VARCHAR(128)    NOT NULL,
    "schema_format"    VARCHAR(128)    NOT NULL,
    "schema_version"   VARCHAR(128)    NOT NULL,
    "root_id"          CHAR(36)        NOT NULL,
    "root_tstamp"      TIMESTAMP       NOT NULL,
    "ref_root"         VARCHAR(255)    NOT NULL,
    "ref_tree"         VARCHAR(1500)   NOT NULL,
    "ref_parent"       VARCHAR(255)    NOT NULL,
    
--  custom columns:
    "timestamp"        TIMESTAMP,
    "date"             DATE,
    "smallint"         SMALLINT,
    "int"              INT,
    "bigint"           BIGINT,
    "double"           DOUBLE PRECISION,
    "decimal_36_2"     DECIMAL(36,2),
    "decimal_no_scale" DECIMAL,
    "boolean"          BOOLEAN,
    "varchar"          VARCHAR(10),
    "char"             CHAR(10)
);

COMMENT ON TABLE atomic.com_test_test_1 IS 'iglu:com.test/test/jsonschema/1-0-0';
