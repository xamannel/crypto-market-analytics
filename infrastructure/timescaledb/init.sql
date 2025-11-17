CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS order_book_snapshots (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    bids JSONB,
    asks JSONB,
    PRIMARY KEY (timestamp, symbol)
);

SELECT create_hypertable('order_book_snapshots', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_symbol_time ON order_book_snapshots (symbol, timestamp DESC);
