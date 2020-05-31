import asyncio
import gc
import os
import socket
import time
import uuid

import pytest

from aioamqp_consumer import Producer


HOST = 'localhost'
PORT = 5672


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
    delay = 1.0

    for i in range(20):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((HOST, PORT))
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
        HOST,
        PORT,
    )


@pytest.fixture
def amqp_queue_name():
    return str(uuid.uuid1())


@pytest.fixture
def producer(amqp_url, loop):
    producer = Producer(amqp_url)

    yield producer

    producer.close()
    loop.run_until_complete(producer.wait_closed())
