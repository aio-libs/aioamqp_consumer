import asyncio
from functools import partial

import pytest
from aioamqp_consumer import Consumer

from tests.conftest import AMQP_QUEUE, AMQP_URL


@pytest.mark.run_loop
@asyncio.coroutine
def test_consumer(producer, loop):
    test_data = [b'test'] * 5
    for data in test_data:
        yield from producer.publish(data, AMQP_QUEUE)

    def task(payload, options, acc=None):
        acc.append(payload)

    test_results = []

    consumer = Consumer(
        AMQP_URL,
        partial(task, acc=test_results),
        AMQP_QUEUE,
        loop=loop,
    )

    yield from consumer.join()

    consumer.close()
    yield from consumer.wait_closed()

    assert test_results == test_data
