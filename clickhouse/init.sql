CREATE TABLE IF NOT EXISTS gaming.raw_events (
    event_timestamp   DateTime,
    event_date        Date DEFAULT toDate(event_timestamp),
    event_name        LowCardinality(String),
    user_id           String,
    username          String,
    session_id        String,
    platform          LowCardinality(String),
    country           LowCardinality(String),
    game_version      LowCardinality(String),
    level             UInt16,
    device_model      String,
    event_params      String
)
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY (event_name, user_id, event_timestamp);

CREATE TABLE IF NOT EXISTS gaming.kafka_raw_events (
    event_timestamp   DateTime,
    event_name        String,
    user_id           String,
    username          String,
    session_id        String,
    platform          String,
    country           String,
    game_version      String,
    level             Int32,
    device_model      String,
    event_params      String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list  = 'processed_raw_events',
    kafka_group_name  = 'clickhouse-raw-events',
    kafka_format      = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS gaming.mv_raw_events TO gaming.raw_events AS
SELECT
    event_timestamp,
    event_name,
    user_id,
    username,
    session_id,
    platform,
    country,
    game_version,
    toUInt16(level) AS level,
    device_model,
    event_params
FROM gaming.kafka_raw_events
WHERE user_id != ''
  AND event_name != '';


CREATE TABLE IF NOT EXISTS gaming.session_metrics (
    window_start      DateTime,
    window_end        DateTime,
    event_name        LowCardinality(String),
    platform          LowCardinality(String),
    country           LowCardinality(String),
    event_count       UInt64,
    unique_users      UInt64,
    unique_sessions   UInt64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toDate(window_start)
ORDER BY (window_start, event_name, platform, country);

CREATE TABLE IF NOT EXISTS gaming.kafka_session_metrics (
    window_start    DateTime,
    window_end      DateTime,
    event_name      String,
    platform        String,
    country         String,
    event_count     Int64,
    unique_users    Int64,
    unique_sessions Int64
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list  = 'processed_session_metrics',
    kafka_group_name  = 'clickhouse-session-metrics',
    kafka_format      = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS gaming.mv_session_metrics TO gaming.session_metrics AS
SELECT
    window_start,
    window_end,
    event_name,
    platform,
    country,
    toUInt64(event_count)     AS event_count,
    toUInt64(unique_users)    AS unique_users,
    toUInt64(unique_sessions) AS unique_sessions
FROM gaming.kafka_session_metrics;


CREATE TABLE IF NOT EXISTS gaming.player_revenue (
    user_id           String,
    username          String,
    country           LowCardinality(String),
    platform          LowCardinality(String),
    total_revenue_usd Decimal(10, 4),
    purchase_count    UInt64
)
ENGINE = SummingMergeTree((total_revenue_usd, purchase_count))
PARTITION BY country
ORDER BY (user_id, platform);

CREATE TABLE IF NOT EXISTS gaming.kafka_player_revenue (
    event_timestamp   DateTime,
    user_id           String,
    username          String,
    country           String,
    platform          String,
    revenue_usd       String,
    item_id           String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list  = 'processed_player_revenue',
    kafka_group_name  = 'clickhouse-player-revenue',
    kafka_format      = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS gaming.mv_player_revenue TO gaming.player_revenue AS
SELECT
    user_id,
    username,
    country,
    platform,
    toDecimal64(toFloat64OrZero(revenue_usd), 4) AS total_revenue_usd,
    toUInt64(1)                                  AS purchase_count
FROM gaming.kafka_player_revenue
WHERE user_id != '';


CREATE TABLE IF NOT EXISTS gaming.dead_letter_events (
    received_at       DateTime DEFAULT now(),
    raw_payload       String,
    error_reason      LowCardinality(String),
    kafka_offset      Int64,
    kafka_partition   Int32
)
ENGINE = MergeTree()
PARTITION BY toDate(received_at)
ORDER BY received_at;

CREATE TABLE IF NOT EXISTS gaming.kafka_dead_letter (
    received_at     DateTime,
    raw_payload     String,
    error_reason    String,
    kafka_offset    Int64,
    kafka_partition Int32
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list  = 'processed_dead_letter',
    kafka_group_name  = 'clickhouse-dead-letter',
    kafka_format      = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS gaming.mv_dead_letter TO gaming.dead_letter_events AS
SELECT
    received_at,
    raw_payload,
    error_reason,
    kafka_offset,
    kafka_partition
FROM gaming.kafka_dead_letter;
