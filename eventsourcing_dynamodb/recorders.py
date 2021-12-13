import time
from typing import List, Optional, Any
from uuid import UUID

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from eventsourcing.persistence import AggregateRecorder, ApplicationRecorder, Notification, StoredEvent, ProcessRecorder

import boto3

import logging

from models import DynamoStoredEvent

logger = logging.getLogger()


class DynamoAggregateRecorder(AggregateRecorder):
    def __init__(self, dynamo_table_name, dynamo_resource):
        self.dynamo_table_name = dynamo_table_name
        self.dynamo_resource = dynamo_resource

    def insert_events(self, stored_events: List[StoredEvent], **kwargs: Any) -> None:
        table = self.dynamo_resource.Table(self.dynamo_table_name)
        with table.batch_writer() as batch:
            for event in stored_events:
                dynamo_event = DynamoStoredEvent(
                    originator_id=str(event.originator_id),
                    originator_version=event.originator_version,
                    topic=event.topic,
                    state=event.state,
                )
                item = {
                    "originator_id" : str(event.originator_id),
                    "originator_version" : event.originator_version,
                    "topic" : event.topic,
                    "state" : event.state,
                    "created_at": int(time.time())
                }
                batch.put_item(Item=item)

    def select_events(self, originator_id: UUID, gt: Optional[int] = None, lte: Optional[int] = None,
                      desc: bool = False, limit: Optional[int] = None) -> List[StoredEvent]:
        # Get records from dynamoDB
        records = self._get_events_from_dynamo(originator_id)
        # Filter
        if gt is not None:
            records = list(filter(lambda x: (x.originator_version > gt), records))
        if lte is not None:
            records = list(filter(lambda x: (x.originator_version <= lte), records))
        records.sort(key=lambda x: x.originator_version, reverse=not desc)
        records = records[0:limit] if limit is not None else list(records)
        stored_events = [
            StoredEvent(
                originator_id=UUID(r.originator_id),
                originator_version=int(r.originator_version),
                topic=r.topic,
                state=r.state
                if isinstance(r.state, memoryview)
                else r.state.value,
            )
            for r in records
        ]
        return stored_events

    def _get_events_from_dynamo(self, originator_id) -> List[DynamoStoredEvent]:
        # Get events for originator_id from dynamodb
        if self.dynamo_resource is None:
            raise ValueError('Unable to use DynamoDB without a dynamodb resource.')

        table = self.dynamo_resource.Table(self.dynamo_table_name)

        key_condition_expression = Key('originator_id').eq(str(originator_id))
        try:
            response = table.query(
                KeyConditionExpression=key_condition_expression
            )
        except ClientError as e:
            logger.exception(
                "Unable to access table %s in dynamo. %s" % (
                    self.dynamo_table_name, e.response.get('Error', {}).get('Message')
                )
            )
            raise ValueError("Unable to access table %s" % (self.dynamo_table_name,))
        else:
            items = response.get('Items', [])

            while response.get("LastEvaluatedKey") is not None:
                response = table.query(
                    KeyConditionExpression=key_condition_expression,
                    ExclusiveStartKey=response.get("LastEvaluatedKey")
                )
                items += response.get('Items', [])

            logger.info(f"Fetched {len(items)} events from database")
            items = [DynamoStoredEvent(**item) for item in items]
            return items


class DynamoApplicationRecorder(DynamoAggregateRecorder, ApplicationRecorder):
    def select_notifications(self, start: int, limit: int) -> List[Notification]:
        raise NotImplementedError()

    def max_notification_id(self) -> int:
        raise NotImplementedError()


class DynamoProcessRecorder(DynamoApplicationRecorder, ProcessRecorder):
    def max_tracking_id(self, application_name: str) -> int:
        raise NotImplementedError()
