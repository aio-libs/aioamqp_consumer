import asyncio

from .compat import PY_350
from .mixins import AMQPMixin


class Producer(AMQPMixin):

    def __init__(
        self,
        amqp_url,
        *,
        loop=None,
        **amqp_kwargs
    ):
        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        self.amqp_url = amqp_url

        self.amqp_kwargs = amqp_kwargs

        self._connect_lock = asyncio.Lock(loop=self.loop)

        self._ensure_queue_lock = asyncio.Lock(loop=self.loop)

        self._known_queues = set()

    @asyncio.coroutine
    def _ensure_queue(self, queue_name, **queue_kwargs):
        with (yield from self._ensure_queue_lock):
            if queue_name in self._known_queues:
                return

            yield from self.queue_declare(queue_name, **queue_kwargs)

            self._known_queues.add(queue_name)

    @asyncio.coroutine
    def _connect(self):
        with (yield from self._connect_lock):
            if not self._connected:
                yield from super()._connect(self.amqp_url, **self.amqp_kwargs)

    @asyncio.coroutine
    def queue_declare(self, queue_name, **queue_kwargs):
        try:
            yield from self._connect()

            result = yield from self._queue_declare(
                queue_name=queue_name,
                **queue_kwargs
            )
            return result
        except:  # noqa
            yield from self._disconnect()
            raise

    @asyncio.coroutine
    def publish(
        self,
        payload,

        queue_name,
        exchange_name='',

        properties=None,
        mandatory=True,
        immediate=False,

        **queue_kwargs
    ):
        assert isinstance(payload, bytes)

        try:
            assert not self._closed, 'Cannot publish while closed'

            yield from self._connect()

            yield from self._ensure_queue(queue_name, **queue_kwargs)

            result = yield from self._basic_publish(
                payload,
                exchange_name=exchange_name,
                routing_key=queue_name,
                properties=properties,
                mandatory=mandatory,
                immediate=immediate,
            )
            return result
        except:  # noqa
            yield from self._disconnect()
            raise

    @asyncio.coroutine
    def queue_purge(self, queue_name, no_wait=False):
        try:
            assert not self._closed, 'Cannot publish while closed'

            yield from self._connect()

            yield from self._queue_purge(queue_name, no_wait=no_wait)
        except:  # noqa
            yield from self._disconnect()
            raise

    @asyncio.coroutine
    def _disconnect(self):
        self._known_queues = set()

        yield from super()._disconnect()

    def close(self):
        self._closed = True

    @asyncio.coroutine
    def wait_closed(self):
        assert self._closed, 'Must be closed first'

        yield from self._disconnect()

    if PY_350:
        @asyncio.coroutine
        def __aenter__(self):  # noqa
            return self

        @asyncio.coroutine
        def __aexit__(self, *exc_info):  # noqa
            self.close()
            yield from self.wait_closed()
