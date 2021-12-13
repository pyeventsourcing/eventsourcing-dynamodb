# -*- coding: utf-8 -*-
import os
from typing import Type

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
)
from eventsourcing.tests.infrastructure_testcases import InfrastructureFactoryTestCase
from eventsourcing.utils import get_topic

from factory import Factory
from recorders import (
    DynamoAggregateRecorder,
    DynamoApplicationRecorder,
    DynamoProcessRecorder
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
        os.environ[InfrastructureFactory.TOPIC] = get_topic(Factory)
        os.environ[Factory.DYNAMO_TABLE] = "dynamo_events"
        os.environ[Factory.ENDPOINT_URL] = "http://localhost:8000"
        super().setUp()

    def tearDown(self) -> None:
        if Factory.DYNAMO_TABLE in os.environ:
            del os.environ[Factory.DYNAMO_TABLE]
        if Factory.ENDPOINT_URL in os.environ:
            del os.environ[Factory.ENDPOINT_URL]
        super().tearDown()


del InfrastructureFactoryTestCase
