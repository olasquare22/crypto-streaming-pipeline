"""
streaming_job.py

Spark Structured Streaming job that:
  1. Reads price events from the Kafka topic "crypto_trades"
  2. Parses the JSON payload
  3. Writes (upserts) each record into Cassandra's crypto_data.latest_prices table

Because Cassandra always writes based on the table's PRIMARY KEY, writing
a new row for a symbol that already exists simply overwrites it - that's
exactly the "latest price per coin" behaviour we want.

This job runs INSIDE the Spark cluster (via spark-submit), not on your
host machine - so it connects to Kafka and Cassandra using their internal
Docker network hostnames ("kafka" and "cassandra"), not localhost.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# ---------------------------------------------------------------------------
# Spark session, configured to know how to reach Cassandra
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CryptoPriceStreaming")
    .config("spark.cassandra.connection.host", "cassandra")
    .getOrCreate()
)

# Reduce log noise - Spark is very chatty at the default INFO level.
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Schema matching the JSON our producer sends to Kafka
# ---------------------------------------------------------------------------

price_event_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),   # may be null - CoinGecko doesn't give us this
    StructField("trade_time", LongType()),   # epoch milliseconds
])

# ---------------------------------------------------------------------------
# Read from Kafka
# ---------------------------------------------------------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")   # internal listener - Spark runs inside the Docker network
    .option("subscribe", "crypto_trades")
    # "earliest" only matters for the very first run ever - once the
    # checkpoint above exists, Spark ignores this and resumes from the
    # checkpoint instead. Using "earliest" here means our first run picks
    # up whatever's already sitting in Kafka, rather than only new messages.
    .option("startingOffsets", "earliest")
    .load()
)

# Kafka gives us raw bytes in a "value" column - cast to string, then parse the JSON.
parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), price_event_schema).alias("data"))
    .select("data.*")
    # trade_time comes in as epoch millis (a plain number) - convert to a
    # real timestamp type so Cassandra stores it correctly.
    .withColumn("trade_time", (col("trade_time") / 1000).cast("timestamp"))
    .withColumn("updated_at", current_timestamp())
)


# ---------------------------------------------------------------------------
# Write each micro-batch to Cassandra
# ---------------------------------------------------------------------------

def write_batch_to_cassandra(batch_df, batch_id):
    count = batch_df.count()
    print(f"--- Batch {batch_id}: {count} record(s) ---")

    if count > 0:
        batch_df.show(truncate=False)

        (
            batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .options(table="latest_prices", keyspace="crypto_data")
            # Default consistency is LOCAL_QUORUM, which caused write
            # timeouts on our single-node, memory-constrained Cassandra
            # instance. LOCAL_ONE only needs the one node we actually have
            # to acknowledge, which matches our real topology.
            .option("spark.cassandra.output.consistency.level", "LOCAL_ONE")
            .mode("append")   # "append" here still upserts, because Cassandra writes by primary key
            .save()
        )


query = (
    parsed_stream.writeStream
    .foreachBatch(write_batch_to_cassandra)
    .outputMode("update")
    # A persistent checkpoint location (instead of the default temporary
    # one) lets Spark remember exactly which Kafka offsets it already
    # processed. Without this, every restart forgets all prior progress
    # and only picks up messages that arrive AFTER the restart - which is
    # why earlier runs seemed to "lose" some coins whenever the job
    # crashed or was resubmitted.
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/crypto_trades")
    .start()
)

query.awaitTermination()