import asyncio
import gc
import os
import socket
import time
import uuid

import pytest

from aioamqp_consumer import Consumer, Producer

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', '5672'))


@pytest.fixture
def event_loop(request):
    loop = asyncio.new_event_loop()
    loop.set_debug(bool(os.environ.get('PYTHONASYNCIODEBUG')))

    yield loop

    loop.call_soon(loop.stop)
    loop.run_forever()

    try:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop._default_executor.shutdown(wait=True)
    except AttributeError:
        pass
    loop.close()

    gc.collect()


@pytest.fixture
def loop(event_loop):
    return event_loop


def probe():
    delay = 0.1

    for i in range(20):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((RABBITMQ_HOST, RABBITMQ_PORT))
            break
        except OSError:
            time.sleep(delay)
        finally:
            s.close()
    else:
        pytest.fail('Cannot reach rabbitmq server')


@pytest.fixture(scope='session')
def amqp_url():
    probe()

    return 'amqp://{}:{}@{}:{}//'.format(
        'guest',
        'guest',
        RABBITMQ_HOST,
        RABBITMQ_PORT,
    )


@pytest.fixture
async def producer(amqp_url):
    producer = Producer(amqp_url)

    await producer.ok()

    yield producer

    producer.close()
    await producer.wait_closed()


@pytest.fixture
def consumer_factory(amqp_url):
    async def wrapper(task, amqp_queue_name):
        consumer = Consumer(
            amqp_url,
            task,
            amqp_queue_name,
        )

        await consumer.ok()

        return consumer

    return wrapper


@pytest.fixture
def consumer_close(consumer_factory, loop):
    consumers = []

    async def wrapper(task, amqp_queue_name):
        consumer = await consumer_factory(task, amqp_queue_name)

        consumers.append(consumer)

        return consumer

    yield wrapper

    async def close(consumer):
        consumer.close()
        await consumer.wait_closed()

    coros = [close(consumer) for consumer in consumers]

    loop.run_until_complete(asyncio.gather(*coros))


@pytest.fixture
def consumer_join(consumer_close):
    async def wrapper(task, amqp_queue_name):
        consumer = await consumer_close(task, amqp_queue_name)

        await consumer.join()

        return consumer

    return wrapper


@pytest.fixture
async def amqp_queue_name(producer):
    queue = str(uuid.uuid1())

    yield queue

    await producer.queue_delete(queue)
