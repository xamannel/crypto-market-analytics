import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
import json
import asyncpg




class OrderBookConsumer:

    def __init__(self, topic, bootstrap_servers, group_id):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False,:asyncio.wait
            auto_offset_reset='latest'
        )

    async def start_consumer(self):

        await self.consumer.start()

    
    async def stop_consumer(self):

        await self.consumer.stop()

    async def connect_db(self):

        # database connection logic 

        pass
    
    async def close_db(self):
        # close database connection logic 

        pass

    async def insert_orderbook(self, message):
        # insert orderbook data into the database
        
        pass

    async def process_batch(self, messages):
        # process batch of messages 
        pass

    async def consume_and_store(self) :

        # Main loop for consuming from Kafka and storing into DB
        
        pass

    async def run(self):
        pass









