-- ReplacingMergeTree collapses rows with the same sorting key, so a window the Flink job sends
-- twice (at-least-once delivery after a restart) is stored once. Query with FINAL for exact counts.

CREATE TABLE IF NOT EXISTS sku_minute_stats
(
    sku            String,
    window_start   DateTime64(3, 'UTC'),
    window_end     DateTime64(3, 'UTC'),
    received       UInt32,
    reserved       UInt32,
    committed      UInt32,
    released       UInt32,
    units_reserved UInt32
)
ENGINE = ReplacingMergeTree
ORDER BY (sku, window_start);

CREATE TABLE IF NOT EXISTS alerts
(
    sku          String,
    rule         LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end   DateTime64(3, 'UTC'),
    reserved     UInt32,
    released     UInt32,
    score        Float64,
    detail       String
)
ENGINE = ReplacingMergeTree
ORDER BY (rule, sku, window_start);
