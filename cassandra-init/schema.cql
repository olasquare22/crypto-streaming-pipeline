-- Run this against Cassandra to set up the keyspace and table

CREATE KEYSPACE IF NOT EXISTS crypto_data
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE crypto_data;

-- One row per symbol, continuously overwritten with the latest trade.
-- symbol is the full partition key, so a write for "BTCUSDT" always
-- upserts the same row instead of creating new ones.
CREATE TABLE IF NOT EXISTS latest_prices (
    symbol      text PRIMARY KEY,
    price       double,
    quantity    double,
    trade_time  timestamp,
    updated_at  timestamp
);