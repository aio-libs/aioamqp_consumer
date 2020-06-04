import pytest

from aioamqp_consumer import JsonPacker


@pytest.mark.asyncio
async def test_consumer_smoke(amqp_queue_name, consumer_join, producer):
    test_data = [b'x'] * 5

    for data in test_data:
        await producer.publish(data, amqp_queue_name)

    test_results = []

    async def task(payload, properties):
        test_results.append(payload)

    await consumer_join(task, amqp_queue_name)

    assert test_results == test_data


@pytest.mark.asyncio
async def test_consumer_json(amqp_queue_name, consumer_close, producer):
    packer = JsonPacker()

    producer.packer = packer

    test_data = [42] * 5

    test_results = []

    async def task(obj, properties):
        test_results.append(obj)

    consumer = await consumer_close(task, amqp_queue_name)

    consumer.packer = packer

    for data in test_data:
        await producer.publish(data, amqp_queue_name)

    await consumer.join()

    assert test_results == test_data
