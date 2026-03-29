CREATE TABLE kafka_source (
    event_timestamp TIMESTAMP(3),
    user_id         STRING,
    username        STRING,
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
    'properties.group.id'          = 'flink-player-revenue',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.fail-on-missing-field'   = 'false',
    'json.ignore-parse-errors'     = 'true'
);

CREATE TABLE kafka_sink_player_revenue (
    event_timestamp STRING,
    user_id         STRING,
    username        STRING,
    country         STRING,
    platform        STRING,
    revenue_usd     STRING,
    item_id         STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'processed_player_revenue',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json'
);

INSERT INTO kafka_sink_player_revenue
SELECT
    DATE_FORMAT(event_timestamp, 'yyyy-MM-dd HH:mm:ss'),
    user_id,
    username,
    country,
    platform,
    JSON_VALUE(event_params, '$.value')   AS revenue_usd,
    JSON_VALUE(event_params, '$.item_id') AS item_id
FROM kafka_source
WHERE event_name = 'purchase'
  AND user_id IS NOT NULL
  AND user_id <> ''
  AND platform IS NOT NULL
  AND platform <> ''
  AND JSON_VALUE(event_params, '$.value') IS NOT NULL
  AND event_timestamp > CAST('2020-01-01 00:00:00' AS TIMESTAMP(3));
