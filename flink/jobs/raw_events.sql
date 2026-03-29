CREATE TABLE kafka_source (
    event_timestamp TIMESTAMP(3),
    user_id         STRING,
    session_id      STRING,
    platform        STRING,
    country         STRING,
    game_version    STRING,
    level           INT,
    event_name      STRING,
    event_params    STRING,
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'game_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-raw-events',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.fail-on-missing-field'   = 'false',
    'json.ignore-parse-errors'     = 'true'
);

CREATE TABLE kafka_sink_raw_events (
    event_timestamp STRING,
    event_name      STRING,
    user_id         STRING,
    session_id      STRING,
    platform        STRING,
    country         STRING,
    game_version    STRING,
    level           INT,
    event_params    STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'processed_raw_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json'
);

INSERT INTO kafka_sink_raw_events
SELECT
    DATE_FORMAT(event_timestamp, 'yyyy-MM-dd HH:mm:ss'),
    event_name,
    user_id,
    session_id,
    platform,
    country,
    game_version,
    level,
    event_params
FROM kafka_source
WHERE user_id IS NOT NULL
  AND user_id <> ''
  AND event_name IS NOT NULL
  AND event_name <> ''
  AND platform IS NOT NULL
  AND platform <> ''
  AND event_timestamp > CAST('2020-01-01 00:00:00' AS TIMESTAMP(3));
