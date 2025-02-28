# Event Sourcing in Python with DynamoDB

This package supports using the Python
[eventsourcing](https://github.com/pyeventsourcing/eventsourcing) library
with [Amazon DynamoDB](https://aws.amazon.com/dynamodb/).

## Table of contents

<!-- TOC -->
* [Table of contents](#table-of-contents)
* [Quick start](#quick-start)
* [Installation](#installation)
* [Getting started](#getting-started)
<!-- TOC -->

## Quick start

To use DynamoDB with your Python eventsourcing applications:
* install the Python package `eventsourcing_dynamodb`
* set the environment variable `PERSISTENCE_MODULE` to `'eventsourcing_dynamodb'`
* set the environment variable `DYNAMO_ENDPOINT_URL` to a DynamoDB endpoint URL
* set the environment variable `DYNAMO_TABLE` to a DynamoDB table

See below for more information.

## Installation

Use pip to install the [stable distribution](https://pypi.org/project/eventsourcing_dynamodb/)
from the Python Package Index. Please note, it is recommended to
install Python packages into a Python virtual environment.

    $ pip install eventsourcing_dynamodb

## Getting started

Define aggregates and applications in the usual way.

```python
from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event
from uuid import uuid5, NAMESPACE_URL


class TrainingSchool(Application):
    def register(self, name):
        dog = Dog(name)
        self.save(dog)

    def add_trick(self, name, trick):
        dog = self.repository.get(Dog.create_id(name))
        dog.add_trick(trick)
        self.save(dog)

    def get_tricks(self, name):
        dog = self.repository.get(Dog.create_id(name))
        return dog.tricks


class Dog(Aggregate):
    @event('Registered')
    def __init__(self, name):
        self.name = name
        self.tricks = []

    @staticmethod
    def create_id(name):
        return uuid5(NAMESPACE_URL, f'/dogs/{name}')

    @event('TrickAdded')
    def add_trick(self, trick):
        self.tricks.append(trick)
```

To use this module as the persistence module for your application, set the environment
variable `PERSISTENCE_MODULE` to `'eventsourcing_dynamodb'`.

When using this module, you need to set the environment variables `DYNAMO_ENDPOINT_URL` and
`DYNAMO_TABLE` to a DynamoDB URL and table.

```python
import os

os.environ['PERSISTENCE_MODULE'] = 'eventsourcing_dynamodb'
os.environ['DYNAMO_ENDPOINT_URL'] = 'http://localhost:8000'
os.environ['DYNAMO_TABLE'] = 'dynamo_events'
```

Construct and use the application in the usual way.

```python
school = TrainingSchool()
school.register('Fido')
school.add_trick('Fido', 'roll over')
school.add_trick('Fido', 'play dead')
tricks = school.get_tricks('Fido')
assert tricks == ['roll over', 'play dead']
```
