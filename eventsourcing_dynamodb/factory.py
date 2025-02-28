# -*- coding: utf-8 -*-
import boto3
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
)
from eventsourcing.utils import Environment

from eventsourcing_dynamodb.recorders import (
    DynamoAggregateRecorder,
    DynamoApplicationRecorder,
    DynamoProcessRecorder,
)


class Factory(InfrastructureFactory):
    """
    Infrastructure factory for DynamoDB infrastructure.
    """

    DYNAMO_TABLE = "DYNAMO_TABLE"
    ENDPOINT_URL = "DYNAMO_ENDPOINT_URL"

    def __init__(self, env: Environment):
        super().__init__(env)
        db_name = self.env.get(self.DYNAMO_TABLE)
        if db_name is None:
            raise EnvironmentError(
                "DynamoDB table not found "
                "in environment with key "
                f"'{self.DYNAMO_TABLE}'"
            )
        endpoint_url = self.env.get(self.ENDPOINT_URL)
        if endpoint_url:
            self.dynamo_resource = boto3.resource("dynamodb", endpoint_url=endpoint_url)
        else:
            self.dynamo_resource = boto3.resource("dynamodb")
        self.dynamo_table_name = db_name

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        recorder = DynamoAggregateRecorder(
            dynamo_table_name=self.dynamo_table_name,
            dynamo_resource=self.dynamo_resource,
        )
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        recorder = DynamoApplicationRecorder(
            dynamo_table_name=self.dynamo_table_name,
            dynamo_resource=self.dynamo_resource,
        )
        return recorder

    def process_recorder(self) -> ProcessRecorder:
        recorder = DynamoProcessRecorder(
            dynamo_table_name=self.dynamo_table_name,
            dynamo_resource=self.dynamo_resource,
        )
        return recorder
