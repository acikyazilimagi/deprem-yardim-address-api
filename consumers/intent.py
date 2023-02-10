# built-in python
import asyncio

# third-party
import logging
import aiokafka
import sentry_sdk

# in-house
from consumers import BaseKafkaClient
from models.conf import KafkaSettings
from config import *
from orjson import loads
from helpers.intent import batch_query

# set logger level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sentry_sdk.init(dsn=SENTRY_DSN)

class Intent(BaseKafkaClient):

    async def process_message(self, record: aiokafka.ConsumerRecord):
        message = record.value
        message_dict = loads(message)
        full_text_list = [i.get("full_text") for i in message_dict if i]

        if not full_text_list:
            logging.warning(f"Raw text is empty, Message: {message_dict}")
            return

        response = batch_query(full_text_list, None)

        if not response:
            logging.warning(f"No response from hugging face endpoint message: {message_dict}")
            return

        await self.producer.send_and_wait(INTENT_TARGET_TOPIC,
                                          response)

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    kafka_settings = KafkaSettings(
        loop=loop,
        client_id=CLIENT_ID,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        max_pool_records=MAX_POOL_RECORDS,
        message_timeout_ms=MESSAGE_TIMEOUT_MS
    )

    try:
        server = Intent(topic=KAFKA_INTENT_TOPIC, server_settings=kafka_settings)
        loop.run_until_complete(server.run())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()