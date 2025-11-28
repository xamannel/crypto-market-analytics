
import os 


# Configuration for KAFKA

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_TOPIC = 'binance-orderbook-raw'
KAFKA_GROUP_ID = 'orderbook-consumer-group'


# DB configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5433))
DB_NAME = os.getenv("DB_NAME", "crypto_market")
DB_USER = os.getenv("DB_USER", "crypto_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "crypto_password")



