import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
import json
import asyncpg
import config
from logger import get_logger
from datetime import datetime

logger = get_logger(__name__)







class OrderBookConsumer:

    def __init__(self):
        self.consumer = None 
        self.db_pool = None

    async def start_consumer(self):

        try :

            self.consumer = AIOKafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=False,
                auto_offset_reset='latest'
            )

            await self.consumer.start()
            logger.info("Kafka consumer started successfully from topic: {}, group: {}".format(config.KAFKA_TOPIC, config.KAFKA_GROUP_ID))

        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")


    
    async def stop_consumer(self):

        if self.consumer is not None :

            await self.consumer.stop()
            logger.info("Kafka consumer stopped.")

    async def connect_db(self):

        try :

            self.db_pool = await asyncpg.create_pool(
                user = config.DB_USER,
                password = config.DB_PASSWORD,
                database = config.DB_NAME,
                host = config.DB_HOST,
                port = config.DB_PORT
            )

            logger.info("Connected to the database successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to the database: {e}")
    
    async def close_db(self):

        if self.db_pool is not None :

            try :
                logger.info("Closing database connection pool...")

                await self.db_pool.close()
                logger.info("Database connection pool closed.")
            except Exception as e:
                logger.error(f"Error closing database connection pool: {e}")
    
    async def process_batch(self, messages):
        
        if self.db_pool is None :
            logger.error("Database pool is not initilialized.")
            return

        insert_query = "INSERT INTO order_book_snapshots (timestamp, symbol, bids, asks) VALUES ($1, $2, $3, $4)"

        async with self.db_pool.acquire() as conn :

            await conn.executemany(insert_query, messages)
            logger.info(f"Inserted batch of {len(messages)} raw order book records into the database.")
            


        

        


        



    async def consume_and_store(self) :
        
        batch = []
        batch_size = 100

        if self.consumer is None :
            logger.error("Consumer is not started.")
            return

        async for message in self.consumer:
            timestamp = datetime.fromisoformat(message.value['timestamp'])
            symbol = message.value['symbol']
            bids = json.dumps(message.value['bids'])
            asks = json.dumps(message.value['asks'])
            batch.append((timestamp, symbol, bids, asks))

            if len(batch) >= batch_size:
                await self.process_batch(batch)
                await self.consumer.commit()
                batch = []



        

    async def run(self):
        await self.connect_db()
        await self.start_consumer()
        try:
            await self.consume_and_store()
        except Exception as e:
            logger.error(f"Error during consumption and storage: {e}")
        finally:
            await self.stop_consumer()
            await self.close_db()


if __name__ == "__main__":
    consumer = OrderBookConsumer()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(consumer.run())








