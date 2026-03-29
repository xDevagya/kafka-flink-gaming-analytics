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
    'properties.group.id'          = 'flink-session-metrics',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.fail-on-missing-field'   = 'false',
    'json.ignore-parse-errors'     = 'true'
);

CREATE TABLE kafka_sink_session_metrics (
    window_start    STRING,
    window_end      STRING,
    event_name      STRING,
    platform        STRING,
    country         STRING,
    event_count     BIGINT,
    unique_users    BIGINT,
    unique_sessions BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'processed_session_metrics',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json'
);

INSERT INTO kafka_sink_session_metrics
SELECT
    DATE_FORMAT(TUMBLE_START(event_timestamp, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss'),
    DATE_FORMAT(TUMBLE_END(event_timestamp, INTERVAL '1' MINUTE),   'yyyy-MM-dd HH:mm:ss'),
    event_name,
    platform,
    country,
    COUNT(*)                    AS event_count,
    COUNT(DISTINCT user_id)     AS unique_users,
    COUNT(DISTINCT session_id)  AS unique_sessions
FROM kafka_source
WHERE user_id IS NOT NULL
  AND user_id <> ''
  AND platform IS NOT NULL
  AND platform <> ''
  AND event_timestamp > CAST('2020-01-01 00:00:00' AS TIMESTAMP(3))
GROUP BY
    TUMBLE(event_timestamp, INTERVAL '1' MINUTE),
    event_name,
    platform,
    country;

