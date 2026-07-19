"""
producer.py

Polls CoinGecko's free public REST API for live prices of a handful of
coins, and publishes each price update as a JSON message to a Kafka topic.

Data flow:
    CoinGecko REST API -> this script (polling loop) -> Kafka topic "crypto_trades"

No API key needed - CoinGecko's /simple/price endpoint is free and public.

Note: we originally built this against Binance's and then Bybit's WebSocket
trade streams, but both were unreachable on this network (DNS timeouts),
consistent with Nigerian mobile carriers blocking crypto exchange domains.
CoinGecko is a market-data aggregator rather than an exchange, resolves fine,
and fits our use case well since we only ever store the latest price per
coin anyway (not full trade-by-trade history).
"""

import time
import requests
from kafka import KafkaProducer
import json

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# CoinGecko coin ids (their internal names, not ticker symbols) mapped to
# the trading-pair-style symbol we want to store downstream.
COINS = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
    "binancecoin": "BNBUSDT",
    "ripple": "XRPUSDT",
}

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

# How often to poll. CoinGecko's free tier allows roughly 10-30 calls/minute;
# one call here fetches ALL 5 coins at once, so polling every 10 seconds
# (6 calls/minute) stays comfortably under that limit.
POLL_INTERVAL_SECONDS = 10

# IMPORTANT: this script runs on your host machine, not inside a Docker
# container. Our docker-compose.yml advertises Kafka's EXTERNAL listener
# at localhost:9094 specifically for this reason. Do not use 9092 here -
# that port is only reachable from other containers on the Docker network.
KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
KAFKA_TOPIC = "crypto_trades"


# ---------------------------------------------------------------------------
# Kafka producer setup
# ---------------------------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    # Explicitly specifying the API version skips kafka-python's automatic
    # version-detection handshake at startup. That handshake occasionally
    # fails on Windows when localhost resolves to IPv6 (::1) instead of
    # IPv4 (127.0.0.1), even though the port itself is genuinely reachable.
    api_version=(3, 6, 0),
)


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

def fetch_prices():
    """Call CoinGecko once for all coins, return a dict like:
    {"bitcoin": {"usd": 67234.5}, "ethereum": {"usd": 3521.1}, ...}
    """
    params = {
        "ids": ",".join(COINS.keys()),
        "vs_currencies": "usd",
    }
    response = requests.get(COINGECKO_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def run():
    print(f"Polling CoinGecko every {POLL_INTERVAL_SECONDS}s for: {', '.join(COINS.values())}")

    while True:
        try:
            prices = fetch_prices()
            now_millis = int(time.time() * 1000)

            for coingecko_id, symbol in COINS.items():
                price_data = prices.get(coingecko_id)
                if not price_data:
                    continue

                price_event = {
                    "symbol": symbol,
                    "price": price_data["usd"],
                    "quantity": None,   # CoinGecko's simple/price endpoint doesn't provide trade size
                    "trade_time": now_millis,
                }

                producer.send(
                    KAFKA_TOPIC,
                    key=price_event["symbol"],
                    value=price_event,
                )

                print(f"Sent: {price_event['symbol']} @ {price_event['price']}")

        except requests.RequestException as e:
            print(f"Request to CoinGecko failed: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()