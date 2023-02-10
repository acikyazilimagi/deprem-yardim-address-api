from fastapi import FastAPI
import threading
import asyncio
from consumers.address_resolve import AddressResolve
from config import *
from models.conf import KafkaSettings
import uvicorn

app = FastAPI()

@app.get("/health-check")
def health_check():
    return {"status": "healthy"}

def start_kafka_server():
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
        server = AddressResolve(topic=KAFKA_ADDRESS_RESOLVE_TOPIC, server_settings=kafka_settings)
        loop.run_until_complete(server.run())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == '__main__':
    kafka_thread = threading.Thread(target=start_kafka_server)
    kafka_thread.start()
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)

