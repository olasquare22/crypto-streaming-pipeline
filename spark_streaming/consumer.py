"""
consumer.py

Reads price events from the Kafka topic "crypto_trades" and writes
(upserts) each one directly into Cassandra's crypto_data.latest_prices
table - no Spark involved.

Data flow:
    Kafka topic "crypto_trades" -> this script -> Cassandra "latest_prices"

Why no Spark: this machine has ~3.48GB of total RAM, which isn't enough
to run Kafka + Cassandra + a Spark master + a Spark worker simultaneously
(each is a separate JVM with its own baseline memory footprint). Since our
transformation logic is simple - parse JSON, reshape a few fields - a
plain Python consumer does the same job without needing a distributed
processing engine at all. Spark earns its keep at real data volumes; for
5 coins polled every 10 seconds, it was always more infrastructure than
the workload needed.

This script runs on your HOST machine (not inside a Docker container),
same as producer.py - so it connects to Kafka via the EXTERNAL listener
(localhost:9094) and to Cassandra via its published port (localhost:9042).
"""

import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
KAFKA_TOPIC = "crypto_trades"

CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "crypto_data"
CASSANDRA_TABLE = "latest_prices"


# ---------------------------------------------------------------------------
# Cassandra connection
# ---------------------------------------------------------------------------

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect(CASSANDRA_KEYSPACE)

upsert_statement = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_TABLE} (symbol, price, quantity, trade_time, updated_at)
    VALUES (?, ?, ?, ?, toTimestamp(now()))
    """
)
# Single-node cluster, so LOCAL_ONE is the right consistency level - it
# only needs the one node we actually have to acknowledge the write.
upsert_statement.consistency_level = ConsistencyLevel.LOCAL_ONE


# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    # Deserialize the raw bytes Kafka gives us back into a Python dict.
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    # "earliest" means a fresh consumer picks up everything already in the
    # topic rather than only new messages. group_id makes offset tracking
    # persistent - if this script restarts, it resumes from where it left
    # off instead of reprocessing (or skipping) messages.
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="crypto-price-consumer",
)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run():
    print(f"Listening on Kafka topic '{KAFKA_TOPIC}', writing to Cassandra '{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}'...")

    for message in consumer:
        event = message.value

        try:
            trade_time = (
                datetime.fromtimestamp(event["trade_time"] / 1000.0, tz=timezone.utc)
                if event.get("trade_time")
                else None
            )

            session.execute(
                upsert_statement,
                (
                    event["symbol"],
                    event["price"],
                    event.get("quantity"),   # may be None - CoinGecko doesn't provide trade size
                    trade_time,
                ),
            )
            print(f"Upserted: {event['symbol']} @ {event['price']}")

        except Exception as e:
            print(f"Failed to write {event.get('symbol', '?')} to Cassandra: {e}")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        cluster.shutdown()