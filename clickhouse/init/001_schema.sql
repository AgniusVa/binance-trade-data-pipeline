CREATE DATABASE IF NOT EXISTS binance;

CREATE TABLE IF NOT EXISTS binance.trades_final
(
    symbol LowCardinality(String),
    trade_id UInt64,
    price Decimal(38, 18),
    qty Decimal(38, 18),
    quote_qty Decimal(38, 18),
    trade_time DateTime64(3, 'UTC'),
    is_buyer_maker Bool,
    is_best_match Bool,
    ingested_at DateTime64(3, 'UTC'),
    source_batch_id UUID
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_time)
ORDER BY (symbol, trade_id);

CREATE TABLE IF NOT EXISTS binance.checkpoint_events
(
    symbol LowCardinality(String),
    next_expected_trade_id UInt64,
    status LowCardinality(String),
    source_batch_id UUID,
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (symbol, event_time);

CREATE TABLE IF NOT EXISTS binance.ingestion_batches
(
    batch_id UUID,
    symbol LowCardinality(String),
    expected_from_id UInt64,
    min_trade_id UInt64,
    max_trade_id UInt64,
    fetched_count UInt32,
    inserted_count UInt32,
    duplicate_count UInt32,
    status LowCardinality(String),
    started_at DateTime64(3, 'UTC'),
    finished_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (symbol, started_at, batch_id);

CREATE TABLE IF NOT EXISTS binance.staged_trades
(
    symbol LowCardinality(String),
    trade_id UInt64,
    price String,
    qty String,
    quote_qty String,
    trade_time_ms UInt64,
    is_buyer_maker Bool,
    is_best_match Bool,
    reason LowCardinality(String),
    source_batch_id UUID,
    staged_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (symbol, trade_id, staged_at);

CREATE TABLE IF NOT EXISTS binance.quality_incidents
(
    incident_id UUID,
    symbol LowCardinality(String),
    code LowCardinality(String),
    severity LowCardinality(String),
    expected_trade_id UInt64,
    observed_min_trade_id UInt64,
    observed_max_trade_id UInt64,
    message String,
    source_batch_id UUID,
    created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (symbol, created_at, incident_id);
