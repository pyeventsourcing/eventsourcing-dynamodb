import os

from eventsourcing.tests.test_application_with_popo import TestApplicationWithPOPO


class TestApplicationWithDynamoDB(TestApplicationWithPOPO):
    expected_factory_topic = "eventsourcing_dynamodb.factory:Factory"
    sqlalchemy_database_url = "sqlite:///:memory:"

    def setUp(self) -> None:
        super().setUp()
        os.environ[
            "INFRASTRUCTURE_FACTORY"
        ] = "eventsourcing_dynamodb.factory:Factory"
        os.environ["DYNAMO_TABLE"] = "dynamo_events"
        os.environ["DYNAMO_ENDPOINT_URL"] = "http://localhost:8000"

    def tearDown(self) -> None:
        del os.environ["INFRASTRUCTURE_FACTORY"]
        del os.environ["DYNAMO_TABLE"]
        del os.environ["DYNAMO_ENDPOINT_URL"]

        super().tearDown()



del TestApplicationWithPOPO
