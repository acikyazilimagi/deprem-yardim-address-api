from models import BaseModel
from typing import List, Optional
from typing import Any

class KafkaConsumerSettings(BaseModel):
    bootstrap_servers: List[str]
    # Kafka project name
    client_id: Optional[str]
    ssl_context: Optional[Any]
    sasl_mechanism: Optional[str]
    sasl_plain_username: Optional[str]
    sasl_plain_password: Optional[str]
    enable_auto_commit: Optional[bool] = True
    security_protocol: Optional[str]
    loop: Any


class KafkaSettings(KafkaConsumerSettings):

    # Max message pool count
    max_pool_records: Optional[int]
    message_timeout_ms: int
