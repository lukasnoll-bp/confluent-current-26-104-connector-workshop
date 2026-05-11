-- ============================================================================
-- ksqlDB Statements – Flatten IEC 104 nested objects array
-- ============================================================================
-- The source connector writes to topic 'iec104' with an embedded JSON schema.
-- Each message has an 'objects' ARRAY<STRUCT> that the JDBC Sink cannot handle.
-- We UNNEST / CROSS JOIN to produce flat records on topic 'IEC104_FLAT'.
-- ============================================================================
-- Drop existing streams and topic first so re-runs on a cluster with
-- persistent Kafka volumes (docker compose down without -v) succeed cleanly.
DROP STREAM IF EXISTS iec104_flat DELETE TOPIC;
DROP STREAM IF EXISTS iec104_raw;

CREATE OR REPLACE STREAM iec104_raw (
    target_id           VARCHAR KEY,
    typeId              INT,
    typeName            VARCHAR,
    commonAddress       INT,
    causeOfTransmission INT,
    isTest              BOOLEAN,
    isNegative          BOOLEAN,
    objects ARRAY<STRUCT<
        ioa          INT,
        timestampMs  BIGINT,
        qualityFlags INT,
        valueFloat   DOUBLE,
        valueInt     INT,
        valueBoolean BOOLEAN,
        valueStr     VARCHAR
    >>
) WITH (
    KAFKA_TOPIC  = 'iec104',
    VALUE_FORMAT = 'JSON'
);

CREATE OR REPLACE STREAM iec104_flat
WITH (
    KAFKA_TOPIC  = 'IEC104_FLAT',
    VALUE_FORMAT = 'AVRO',
    PARTITIONS   = 1
) AS
SELECT
    AS_VALUE(target_id)            AS target_id,
    typeId,
    typeName,
    commonAddress,
    causeOfTransmission,
    isTest,
    isNegative,
    EXPLODE(objects)->ioa          AS ioa,
    EXPLODE(objects)->timestampMs  AS timestampMs,
    EXPLODE(objects)->qualityFlags AS qualityFlags,
    EXPLODE(objects)->valueFloat   AS valueFloat,
    EXPLODE(objects)->valueInt     AS valueInt,
    EXPLODE(objects)->valueBoolean AS valueBoolean,
    EXPLODE(objects)->valueStr     AS valueStr
FROM iec104_raw
PARTITION BY NULL
EMIT CHANGES;
