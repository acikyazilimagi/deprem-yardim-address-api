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

print(GOOGLE_API_KEY)
print(OPENAI_API_KEY)
address_api = AddressAPI(GOOGLE_API_KEY, OPENAI_API_KEY, NER_API_KEY)

class AddressResolve(BaseKafkaClient):

    async def process_message(self, record: aiokafka.ConsumerRecord):
        message = record.value

        messageIo = BytesIO(message)
        address_df = pd.read_json(messageIo)
        address_df_replica = address_df.copy()
        print(address_df.head())
        regex_results = pd.DataFrame(
            [address_api.regex_api_request(raw_text, entry_id) for
             raw_text, entry_id in
             zip(address_df.raw_text.values, address_df.id.values)])
        regex_to_geocode = regex_results[regex_results.ws >= 0.7]
        del regex_results

        # Ner Process
        address_df = address_df[~address_df.id.isin(regex_to_geocode.id.values)]
        with ThreadPool(60) as executor:
            ner_results = executor.map(
                lambda p: address_api.ner_api_request(*p),
                zip(address_df.raw_text.values,
                    address_df.id.values))
        ner_results = pd.DataFrame(ner_results)
        ner_to_geocode = ner_results[ner_results.ws >= 0.5]
        del ner_results

        geocode_data = pd.concat([regex_to_geocode[['address', 'id']],
                                  ner_to_geocode[['address', 'id']]], axis=0)
        del regex_to_geocode, ner_to_geocode

        with ThreadPool(60) as executor:
            geocode_data = executor.map(
                lambda p: address_api.google_geocode_api_request(*p),
                zip(geocode_data.address.values, geocode_data.id.values))

        geocode_data = pd.DataFrame(geocode_data)
        print(geocode_data.head())
        geocode_data = pd.merge(geocode_data[geocode_data.is_resolved == True],
                                address_df_replica, on='id', how='left')
        del address_df_replica

        final_data = []
        for d in geocode_data.iterrows():
            d = d[1]
            final_data.append(
                {
                    'location': {
                        "formatted_address": d.get('formatted_address', ''),
                        "latitude": d.get('latitude', 0.0),
                        "longitude": d.get('longitude', 0.0),
                        "northeast_lat": d.get('northeast_lat', 0.0),
                        "northeast_lng": d.get('northeast_lng', 0.0),
                        "southwest_lat": d.get('southwest_lat', 0.0),
                        "southwest_lng": d.get('southwest_lng', 0.0),
                        "entry_id": d.get('id'),
                        "epoch": d.get('epoch'),
                        "channel": d.get('channel')},
                    'feed': {
                        "id": d.get('id'),
                        "raw_text": d.get('raw_text'),
                        "channel": d.get('channel'),
                        "extra_parameters": d.get('extra_parameters', {}),
                        "epoch": d.get('epoch')}
                }
            )

        await self.producer.send_and_wait(KAFKA_PROCESSED_TOPIC,
                                          orjson.dumps(final_data))
        logger.info("Message Processed.")
