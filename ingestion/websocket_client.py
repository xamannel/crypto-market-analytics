import asyncio
import json
import logging
from datetime import datetime
from binance import AsyncClient, BinanceSocketManager
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('websocket_client.log'),  # Write to file
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'binance-orderbook-raw'
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'LINKUSDT']  # Symbols to monitor
DEPTH_LEVELS = 10  # Number of order book levels


class BinanceOrderBookProducer:
    """Fetches order book data from Binance and produces to Kafka"""

    def __init__(self, symbols, kafka_bootstrap_servers, kafka_topic, depth=10):
        self.symbols = symbols
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.depth = depth
        self.producer = None
        self.client = None

    async def start_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type='gzip'
            )
            await self.producer.start()
            logger.info(f"Kafka producer started: {self.kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise

    async def stop_producer(self):
        """Stop Kafka producer gracefully"""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")

    async def produce_message(self, message):
        """Send message to Kafka topic"""
        try:
            await self.producer.send_and_wait(self.kafka_topic, message)
            logger.debug(f"Message sent to Kafka: {message['symbol']} at {message['timestamp']}")
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
        except Exception as e:
            logger.error(f"Unexpected error sending to Kafka: {e}")

    async def process_depth_message(self, msg, symbol):
        """Process and enrich raw order book data"""
        try:
            # Enrich message with metadata
            enriched_message = {
                'timestamp': datetime.utcnow().isoformat(),
                'symbol': symbol,
                'event_time': msg.get('E'),  # Binance event time
                'last_update_id': msg.get('u'),  # Last update ID
                'bids': msg.get('bids', []),
                'asks': msg.get('asks', [])
            }

            await self.produce_message(enriched_message)
            print(enriched_message)  # Print to console for monitoring

        except Exception as e:
            logger.error(f"Error processing depth message for {symbol}: {e}")

    async def stream_symbol(self, symbol):
        """Stream order book data for a single symbol"""
        while True:
            try:
                logger.info(f"Starting stream for {symbol}")

                # Create Binance client and socket manager
                client = await AsyncClient.create()
                bsm = BinanceSocketManager(client)

                # Connect to depth stream
                depth_socket = bsm.depth_socket(symbol, depth=self.depth)

                async with depth_socket as stream:
                    logger.info(f"Connected to {symbol} order book stream")

                    while True:
                        msg = await stream.recv()
                        await self.process_depth_message(msg, symbol)

            except asyncio.CancelledError:
                logger.info(f"Stream cancelled for {symbol}")
                break
            except Exception as e:
                logger.error(f"Error in {symbol} stream: {e}")
                logger.info(f"Reconnecting {symbol} in 5 seconds...")
                await asyncio.sleep(5)
            finally:
                if client:
                    await client.close_connection()

    async def run(self):
        """Start producing order book data for all symbols"""
        await self.start_producer()

        try:
            # Create tasks for all symbols
            tasks = [
                asyncio.create_task(self.stream_symbol(symbol))
                for symbol in self.symbols
            ]

            logger.info(f"Monitoring {len(self.symbols)} symbols: {', '.join(self.symbols)}")

            # Run all tasks concurrently
            await asyncio.gather(*tasks)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Error in main run loop: {e}")
        finally:
            await self.stop_producer()


async def main():
    """Main entry point"""
    producer = BinanceOrderBookProducer(
        symbols=SYMBOLS,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        depth=DEPTH_LEVELS
    )

    await producer.run()


if __name__ == "__main__":
    asyncio.run(main())

