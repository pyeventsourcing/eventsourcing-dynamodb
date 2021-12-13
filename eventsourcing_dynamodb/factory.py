from typing import Mapping

import boto3
from eventsourcing.persistence import InfrastructureFactory, AggregateRecorder, ApplicationRecorder, ProcessRecorder

from recorders import DynamoProcessRecorder, DynamoApplicationRecorder, DynamoAggregateRecorder


class Factory(InfrastructureFactory):

    DYNAMO_TABLE = "DYNAMO_TABLE"
    ENDPOINT_URL = "DYNAMO_ENDPOINT_URL"

    def __init__(self, application_name: str, env: Mapping[str, str]):
        super().__init__(application_name, env)
        db_name = self.getenv(self.DYNAMO_TABLE)
        if db_name is None:
            raise EnvironmentError(
                "DynamoDB table not found "
                "in environment with key "
                f"'{self.DYNAMO_TABLE}'"
            )
        endpoint_url = self.getenv(self.ENDPOINT_URL)
        if endpoint_url:
            self.dynamo_resource = boto3.resource('dynamodb', endpoint_url=endpoint_url)
        else:
            self.dynamo_resource = boto3.resource('dynamodb')
        self.dynamo_table_name = db_name

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        recorder = DynamoAggregateRecorder(
            dynamo_table_name= self.dynamo_table_name,
            dynamo_resource= self.dynamo_resource
        )
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        recorder = DynamoApplicationRecorder(
            dynamo_table_name=self.dynamo_table_name,
            dynamo_resource=self.dynamo_resource
        )
        return recorder

    def process_recorder(self) -> ProcessRecorder:
        recorder = DynamoProcessRecorder(
            dynamo_table_name= self.dynamo_table_name,
            dynamo_resource= self.dynamo_resource
        )
        return recorder

