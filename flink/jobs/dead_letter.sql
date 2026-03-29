CREATE TABLE kafka_source (
    event_timestamp TIMESTAMP(3),
    user_id         STRING,
    session_id      STRING,
    platform        STRING,
    country         STRING,
    game_version    STRING,
    level           INT,
    event_name      STRING,
    device_model    STRING,
    event_params    STRING,
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'game_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-dead-letter',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.fail-on-missing-field'   = 'false',
    'json.ignore-parse-errors'     = 'true'
);

CREATE TABLE kafka_sink_dead_letter (
    received_at     STRING,
    raw_payload     STRING,
    error_reason    STRING,
    kafka_offset    BIGINT,
    kafka_partition INT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'processed_dead_letter',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'properties.group.id'          = 'flink-dead-letter-sink'
);

INSERT INTO kafka_sink_dead_letter
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS received_at,
    CONCAT(
        '{"user_id":"',   COALESCE(user_id, ''),
        '","event_name":"', COALESCE(event_name, ''),
        '","platform":"',   COALESCE(platform, ''),
        '"}'
    )                                                      AS raw_payload,
    CASE
        WHEN user_id IS NULL OR user_id = ''       THEN 'missing_user_id'
        WHEN event_name IS NULL OR event_name = '' THEN 'missing_event_name'
        WHEN platform IS NULL OR platform = ''     THEN 'missing_platform'
        WHEN event_timestamp <= CAST('2020-01-01 00:00:00' AS TIMESTAMP(3)) THEN 'invalid_timestamp'
        ELSE 'unknown'
    END                                                    AS error_reason,
    CAST(-1 AS BIGINT)                                     AS kafka_offset,
    CAST(-1 AS INT)                                        AS kafka_partition
FROM kafka_source
WHERE user_id IS NULL
   OR user_id = ''
   OR event_name IS NULL
   OR event_name = ''
   OR platform IS NULL
   OR platform = ''
   OR event_timestamp <= CAST('2020-01-01 00:00:00' AS TIMESTAMP(3));
