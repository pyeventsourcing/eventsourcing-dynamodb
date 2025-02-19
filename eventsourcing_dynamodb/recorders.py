import logging
import time
from typing import Any, List, Sequence
from uuid import UUID

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    Notification,
    ProcessRecorder,
    StoredEvent,
)

from eventsourcing_dynamodb.models import DynamoStoredEvent

logger = logging.getLogger()


class DynamoAggregateRecorder(AggregateRecorder):

    def __init__(self, dynamo_table_name, dynamo_resource):
        self.dynamo_table_name = dynamo_table_name
        self.dynamo_resource = dynamo_resource

    def insert_events(self, stored_events: List[StoredEvent],
                      **kwargs: Any) -> None:
        table = self.dynamo_resource.Table(self.dynamo_table_name)
        with table.batch_writer() as batch:
            for event in stored_events:
                item = {
                    "originator_id": str(event.originator_id),
                    "originator_version": event.originator_version,
                    "topic": event.topic,
                    "state": event.state,
                    "created_at": int(time.time()),
                }
                batch.put_item(Item=item)

    def select_events(
        self,
        originator_id: UUID,
        *,
        gt: int | None = None,
        lte: int | None = None,
        desc: bool = False,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        # Get records from dynamoDB
        records = self._get_events_from_dynamo(originator_id, gt, lte, desc,
                                               limit)
        stored_events = [
            StoredEvent(
                originator_id=UUID(r.originator_id),
                originator_version=r.originator_version,
                topic=r.topic,
                state=r.state
                if isinstance(r.state, memoryview) else r.state.value,
            ) for r in records
        ]
        return stored_events

    def _get_events_from_dynamo(
        self,
        originator_id: str,
        gt: int | None = None,
        lte: int | None = None,
        desc: bool = False,
        limit: int | None = None,
    ) -> List[DynamoStoredEvent]:
        # Get events for originator_id from dynamodb
        if self.dynamo_resource is None:
            raise ValueError(
                'Unable to use DynamoDB without a dynamodb resource.')

        table = self.dynamo_resource.Table(self.dynamo_table_name)

        key_condition_expression = Key('originator_id').eq(str(originator_id))

        if gt is not None:
            key_condition_expression &= Key('originator_version').gt(gt)

        if lte is not None:
            key_condition_expression &= Key('originator_version').lte(lte)

        query_kwargs = {
            "KeyConditionExpression": key_condition_expression,
            "ScanIndexForward": not desc,
        }

        if limit is not None:
            query_kwargs["Limit"] = limit

        try:
            done = False
            start_key = None
            items = []
            while not done:
                if start_key:
                    query_kwargs["ExclusiveStartKey"] = start_key
                response = table.query(**query_kwargs)
                items += response.get('Items', [])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
            logger.info(f"Fetched {len(items)} events from database")
            return [DynamoStoredEvent(**item) for item in items]

        except ClientError as e:
            logger.exception("Unable to access table %s in dynamo. %s" %
                             (self.dynamo_table_name, e.response.get(
                                 'Error', {}).get('Message')))
            raise ValueError("Unable to access table %s" %
                             (self.dynamo_table_name,))


class DynamoApplicationRecorder(DynamoAggregateRecorder, ApplicationRecorder):

    def select_notifications(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> list[Notification]:
        raise NotImplementedError()

    def max_notification_id(self) -> int:
        raise NotImplementedError()


class DynamoProcessRecorder(DynamoApplicationRecorder, ProcessRecorder):

    def max_tracking_id(self, application_name: str) -> int:
        raise NotImplementedError()

    def has_tracking_id(self, application_name: str,
                        notification_id: int) -> bool:
        raise NotImplementedError()
