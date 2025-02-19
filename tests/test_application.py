import os
from decimal import Decimal
from itertools import chain
from uuid import uuid4

from eventsourcing.application import Application, EventSourcedLog
from eventsourcing.domain import Aggregate, DomainEvent
from eventsourcing.system import NotificationLogReader
from eventsourcing.tests.application import (
    TIMEIT_FACTOR,
    BankAccounts,
    ExampleApplicationTestCase,
)
from eventsourcing.tests.domain import BankAccount
from eventsourcing.utils import get_topic


class TestApplicationWithDynamoDB(ExampleApplicationTestCase):
    timeit_number = 30 * TIMEIT_FACTOR
    expected_factory_topic = "eventsourcing_dynamodb.factory:Factory"

    def setUp(self) -> None:
        self.original_initial_version = Aggregate.INITIAL_VERSION
        Aggregate.INITIAL_VERSION = 1
        super().setUp()
        os.environ["PERSISTENCE_MODULE"] = "eventsourcing_dynamodb"
        os.environ["DYNAMO_TABLE"] = "dynamo_events"
        os.environ["DYNAMO_ENDPOINT_URL"] = "http://localhost:8000"
        os.environ["IS_SNAPSHOTTING_ENABLED"] = "false"

    def tearDown(self) -> None:
        Aggregate.INITIAL_VERSION = self.original_initial_version
        try:
            del os.environ["PERSISTENCE_MODULE"]
        except KeyError:
            pass
        try:
            del os.environ["DYNAMO_TABLE"]
        except KeyError:
            pass
        try:
            del os.environ["DYNAMO_ENDPOINT_URL"]
        except KeyError:
            pass
        try:
            del os.environ["IS_SNAPSHOTTING_ENABLED"]
        except KeyError:
            pass
        super().tearDown()

    def test_example_application(self) -> None:
        app = BankAccounts()

        # Check the factory topic.
        self.assertEqual(get_topic(type(app.factory)),
                         self.expected_factory_topic)

        # Check AccountNotFound exception.
        with self.assertRaises(BankAccounts.AccountNotFoundError):
            app.get_account(uuid4())

        # Open an account.
        account_id1 = app.open_account(
            full_name="Alice",
            email_address="alice@example.com",
        )

        # Check balance.
        self.assertEqual(
            app.get_balance(account_id1),
            Decimal("0.00"),
        )

        # Credit the account.
        app.credit_account(account_id1, Decimal("10.00"))

        # Check balance.
        self.assertEqual(
            app.get_balance(account_id1),
            Decimal("10.00"),
        )

        # Credit the account.
        app.credit_account(account_id1, Decimal("25.00"))

        # Check balance.
        self.assertEqual(
            app.get_balance(account_id1),
            Decimal("35.00"),
        )

        # Credit the account.
        app.credit_account(account_id1, Decimal("30.00"))

        # Check balance.
        self.assertEqual(
            app.get_balance(account_id1),
            Decimal("65.00"),
        )

        # Get historical version.
        account: BankAccount = app.repository.get(account_id1, version=2)
        self.assertEqual(account.version, 2)
        self.assertEqual(account.balance, Decimal("10.00"))

        # # Take snapshot (don't specify version).
        # app.take_snapshot(account_id1)
        # assert app.snapshots
        # snapshots = list(app.snapshots.get(account_id1))
        # self.assertEqual(len(snapshots), 1)
        # self.assertEqual(snapshots[0].originator_version, Aggregate.INITIAL_VERSION + 3)

        # # Get historical version again (this won't use snapshots).
        # historical_account: BankAccount = app.repository.get(account_id1, version=1)
        # self.assertEqual(historical_account.version, 1)

        # # Get current version (this will use snapshots).
        # from_snapshot: BankAccount = app.repository.get(account_id1)
        # self.assertIsInstance(from_snapshot, BankAccount)
        # self.assertEqual(from_snapshot.version, Aggregate.INITIAL_VERSION + 3)
        # self.assertEqual(from_snapshot.balance, Decimal("65.00"))

        # # Take snapshot (specify earlier version).
        # app.take_snapshot(account_id1, version=1)
        # app.take_snapshot(account_id1, version=2)
        # snapshots = list(app.snapshots.get(account_id1))

        # # Shouldn't have recorded historical snapshot (would append old after new).
        # self.assertEqual(len(snapshots), 1)
        # self.assertEqual(snapshots[0].originator_version, Aggregate.INITIAL_VERSION + 3)

        # Open another account.
        account_id2 = app.open_account(
            full_name="Bob",
            email_address="bob@example.com",
        )
        # Credit the account three times.
        app.credit_account(account_id2, Decimal("10.00"))
        app.credit_account(account_id2, Decimal("25.00"))
        app.credit_account(account_id2, Decimal("30.00"))

        # # Snapshot the account.
        # app.take_snapshot(account_id2)

        # Open another account.
        account_id3 = app.open_account(
            full_name="Bob",
            email_address="bob@example.com",
        )
        # Credit the account three times.
        app.credit_account(account_id3, Decimal("10.00"))
        app.credit_account(account_id3, Decimal("25.00"))
        app.credit_account(account_id3, Decimal("30.00"))

    def test_event_sourced_log(self) -> None:

        class LoggedEvent(DomainEvent):
            name: str

        app = Application()
        log = EventSourcedLog(
            events=app.events,
            originator_id=uuid4(),
            logged_cls=LoggedEvent,
        )
        event = log.trigger_event(name="name1")
        app.save(event)

        events = list(log.get())
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].name, "name1")

        event = log.trigger_event(name="name2")
        app.save(event)

        events = list(log.get())
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].name, "name1")
        self.assertEqual(events[1].name, "name2")


del ExampleApplicationTestCase
