import time
import uuid
from dataclasses import dataclass

from boto3.dynamodb.types import Binary


@dataclass(frozen=True)
class DynamoStoredEvent:
    originator_id: str
    originator_version: int
    topic: str
    state: Binary
    created_at: int = int(time.time())
