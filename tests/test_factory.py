# -*- coding: utf-8 -*-
import os
from typing import Type

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
)
from eventsourcing.tests.persistence import InfrastructureFactoryTestCase
from eventsourcing.utils import Environment

from eventsourcing_dynamodb.factory import Factory
from eventsourcing_dynamodb.recorders import (
    DynamoAggregateRecorder,
    DynamoApplicationRecorder,
    DynamoProcessRecorder,
)


class TestFactory(InfrastructureFactoryTestCase):
    def test_create_aggregate_recorder(self) -> None:
        super().test_create_aggregate_recorder()

    def expected_factory_class(self) -> Type[InfrastructureFactory]:
        return Factory

    def expected_aggregate_recorder_class(self) -> Type[AggregateRecorder]:
        return DynamoAggregateRecorder

    def expected_application_recorder_class(self) -> Type[ApplicationRecorder]:
        return DynamoApplicationRecorder

    def expected_process_recorder_class(self) -> Type[ProcessRecorder]:
        return DynamoProcessRecorder

    def setUp(self) -> None:
        self.env = Environment("TestCase")
        self.env[InfrastructureFactory.PERSISTENCE_MODULE] = Factory.__module__
        self.env[Factory.DYNAMO_TABLE] = "dynamo_events"
        self.env[Factory.ENDPOINT_URL] = "http://localhost:8000"
        super().setUp()

    def tearDown(self) -> None:
        if Factory.DYNAMO_TABLE in os.environ:
            del os.environ[Factory.DYNAMO_TABLE]
        if Factory.ENDPOINT_URL in os.environ:
            del os.environ[Factory.ENDPOINT_URL]
        super().tearDown()


del InfrastructureFactoryTestCase
