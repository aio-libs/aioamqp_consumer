import asyncio

import pytest
from aioamqp_consumer import Consumer

from tests.conftest import AMQP_QUEUE, AMQP_URL


@pytest.mark.run_loop
@asyncio.coroutine
def test_consumer_smoke(producer, loop):
    test_data = [b'test'] * 5

    for data in test_data:
        yield from producer.publish(data, AMQP_QUEUE)

    test_results = []

    @asyncio.coroutine
    def task(payload, options):
        test_results.append(payload)

    consumer = Consumer(
        AMQP_URL,
        task,
        AMQP_QUEUE,
        loop=loop,
    )

    yield from consumer.join()

    consumer.close()
    yield from consumer.wait_closed()

    assert test_results == test_data
