import logging
from models.conf import KafkaSettings, KafkaConsumerSettings
import aiokafka

class BaseKafkaClient:
    def __init__(self, server_settings: KafkaSettings, topic: str):
        self.settings = server_settings
        consumer_settings = KafkaConsumerSettings(**server_settings.dict())
        # init consumer & producer
        self.consumer = aiokafka.AIOKafkaConsumer(topic,
                                                  **consumer_settings.dict(
                                                      exclude_none=True))
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=server_settings.bootstrap_servers,
            loop=server_settings.loop)

    async def _process_message(self, record: aiokafka.ConsumerRecord):
        try:
            await self.process_message(record)
        except Exception as exc:
            logging.error(exc)
            logging.error(str(exc))

    async def process_message(self, record: aiokafka.ConsumerRecord):
        ...

    async def process(self):
        data = await self.consumer.getmany(
            timeout_ms=self.settings.message_timeout_ms,
            max_records=self.settings.max_pool_records)
        for topic, records in data.items():
            for record in records:
                try:
                    await self._process_message(record)
                except Exception as exc:
                    logging.error(str(exc))

    async def run(self):
        self.running = True
        try:
            logging.info("Kafka Consumer Started...")
            await self.consumer.start()
            await self.producer.start()
            while self.running:
                await self.process()
        finally:
            logging.info("Kafka Consumer Stopped...")
            await self.consumer.stop()
            await self.producer.stop()

