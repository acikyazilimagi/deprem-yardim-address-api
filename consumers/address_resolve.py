# built-in python
import asyncio
from io import BytesIO
from multiprocessing.pool import ThreadPool
import logging
import sys

# third-party
import orjson
import pandas as pd
import aiokafka

# in-house
from consumers import BaseKafkaClient
from models.conf import KafkaSettings
from config import *
from address_resolver import AddressAPI

# init logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

address_api = AddressAPI(GOOGLE_API_KEY, OPENAI_API_KEY, NER_API_KEY)

class AddressResolve(BaseKafkaClient):

    async def process_message(self, record: aiokafka.ConsumerRecord):
        message = record.value

        #messageIo = BytesIO(message)
        row_data = orjson.loads(message)
        
        regex_results = address_api.regex_api_request(row_data['raw_text'], row_data['id'])
        if regex_results['ws'] >= 0.7:
            geocode_results = address_api.google_geocode_api_request(row_data['raw_text'], row_data['id'])
        else:
            ner_results = address_api.ner_api_request(row_data['raw_text'], row_data['id'])
            if ner_results['ws'] >= 0.5:
                geocode_results = address_api.google_geocode_api_request(row_data['raw_text'], row_data['id'])
            else:
                # Ner veya Regex Çözümleyemedi. TO-Do: OpenAI eklenebilir.
                pass

        final_data = {
         'location':{
            "formatted_address": geocode_results.get('formatted_address', ''),
            "latitude": geocode_results.get('latitude', 0.0),
            "longitude": geocode_results.get('longitude', 0.0),
            "northeast_lat": geocode_results.get('northeast_lat', 0.0),
            "northeast_lng": geocode_results.get('northeast_lng', 0.0),
            "southwest_lat": geocode_results.get('southwest_lat', 0.0),
            "southwest_lng": geocode_results.get('southwest_lng', 0.0),
            "entry_id": row_data.get('id'),
            "epoch": row_data.get('epoch'),
            "channel": row_data.get('channel')},
          'feed': {
            "id": row_data.get('id'),
            "raw_text": row_data.get('raw_text'),
            "channel": row_data.get('channel'),
            "extra_parameters": row_data.get('extra_parameters', {}),
            "epoch": row_data.get('epoch')}}
    

        await self.producer.send_and_wait(KAFKA_PROCESSED_TOPIC,
                                          orjson.dumps(final_data))
        logger.info("Message Processed.")
