# Crypto Price Streaming Pipeline

A real-time data pipeline that polls live cryptocurrency prices, streams them through Kafka, and stores the latest price per coin in Cassandra — visualized in Grafana.

```
CoinGecko (REST API, polled every 10s)
        │
        ▼
   producer.py  ──────►  Kafka topic: crypto_trades
                                  │
                                  ▼
                          consumer.py
                                  │
                                  ▼
                    Cassandra: crypto_data.latest_prices
                                  │
                                  ▼
                              Grafana
```

## What it does

Tracks live USD prices for 5 coins — **BTC, ETH, SOL, BNB, XRP** — and keeps a continuously-updated "latest price" snapshot per coin in a Cassandra table, visualized on a live Grafana dashboard.

## Stack

| Component | Role |
|---|---|
| **Python** | Producer (polls CoinGecko) and consumer (Kafka → Cassandra) |
| **Kafka** | Message broker, decouples ingestion from storage |
| **Cassandra** | Stores the latest price per coin (upsert by primary key) |
| **Grafana** | Dashboard for visualizing current prices |
| **Docker Compose** | Runs Kafka, Cassandra, and Grafana as containers |

## Why no Spark

This started as a Kafka → Spark → Cassandra pipeline, per the original brief. During development it became clear that running a full Spark cluster (master + worker) alongside Kafka and Cassandra needs more memory than a typical dev laptop comfortably provides — each is a separate JVM with its own baseline footprint.

Since the actual transformation logic here is simple (parse JSON, reshape a few fields, upsert), a lightweight Python consumer does the same job as a Spark Structured Streaming job would, without the operational overhead. Spark earns its keep at real data volumes needing distributed processing — for 5 coins polled every 10 seconds, it was always more infrastructure than the workload needed. This is a deliberate architecture decision made after hitting real resource constraints, not a shortcut.

## Why CoinGecko instead of a crypto exchange

The original design used Binance's WebSocket for true tick-by-tick trade data. Binance (and later, Bybit, tried as an alternative) were both unreachable during development due to exchange-domain blocking at the network level. CoinGecko, a market-data aggregator rather than an exchange, was used instead — polled every 10 seconds via REST. This fits the project's actual goal well, since only the latest price per coin is ever stored.

## Running it locally

Full setup, troubleshooting, and a step-by-step restart procedure are documented in [`RESTART_GUIDE.md`](./RESTART_GUIDE.md).

Quick start:

```bash
docker compose up -d
# create the Kafka topic and Cassandra schema (see RESTART_GUIDE.md)
python producer/producer.py       # terminal 1
python spark_streaming/consumer.py  # terminal 2
```

Then open Grafana at `http://localhost:3000` and query the `latest_prices` table via the Cassandra data source.

## Project structure

```
├── docker-compose.yml       # Kafka, Cassandra, Grafana
├── cassandra-init/
│   └── schema.cql           # Keyspace + table definition
├── producer/
│   └── producer.py          # Polls CoinGecko, publishes to Kafka
├── spark_streaming/
│   └── consumer.py          # Kafka consumer, writes to Cassandra
├── requirements.txt
└── RESTART_GUIDE.md          # Full setup + troubleshooting reference
```
