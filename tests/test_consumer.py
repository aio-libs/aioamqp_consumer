import pytest

from aioamqp_consumer import Consumer, JsonPacker


@pytest.mark.asyncio
async def test_consumer_smoke(producer, amqp_queue_name, amqp_url):
    packer = JsonPacker()

    producer.packer = packer

    test_data = [None] * 5

    for data in test_data:
        await producer.publish(data, amqp_queue_name)

    test_results = []

    async def task(obj, properties):
        test_results.append(obj)

    async with Consumer(
        amqp_url,
        task,
        amqp_queue_name,
        packer=packer,
    ) as consumer:
        await consumer.join()

    assert test_results == test_data
