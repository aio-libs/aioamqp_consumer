import asyncio

import pytest
from aioamqp_consumer import Producer

AMQP_URL = 'amqp://guest:guest@127.0.0.1:5672//'
AMQP_QUEUE = 'test'


@pytest.fixture
def loop(request):
    with pytest.raises(RuntimeError):
        asyncio.get_event_loop()

    loop = asyncio.new_event_loop()

    loop.set_debug(True)

    request.addfinalizer(lambda: asyncio.set_event_loop(None))

    try:
        yield loop
    finally:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()


@pytest.fixture
def producer(loop):
    producer = Producer(AMQP_URL, loop=loop)

    try:
        yield producer
    finally:
        producer.close()
        loop.run_until_complete(producer.wait_closed())


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        item = pytest.Function(name, parent=collector)

        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs

        loop = funcargs['loop']

        testargs = {
            arg: funcargs[arg]
            for arg in pyfuncitem._fixtureinfo.argnames
        }

        assert asyncio.iscoroutinefunction(pyfuncitem.obj)

        loop.run_until_complete(pyfuncitem.obj(**testargs))

        return True
