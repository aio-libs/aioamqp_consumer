import asyncio
from functools import wraps

from aioamqp import AioamqpException

from .amqp import AMQPMixin
from .packer import PackerMixin


class Producer(
    PackerMixin,
    AMQPMixin,
):

    def __init__(
        self,
        amqp_url,
        *,
        amqp_kwargs=None,
        packer=None,
        packer_cls=None,
        _no_packer=False,
    ):
        if amqp_kwargs is None:
            amqp_kwargs = {}

        self.loop = asyncio.get_event_loop()

        self.amqp_url = amqp_url

        self.amqp_kwargs = amqp_kwargs

        self._connect_lock = asyncio.Lock()

        self._init_ensure_locks()
        self._init_known()

        self.publish = self._connection_guard(self.publish)
        self.queue_declare = self._connection_guard(self.queue_declare)
        self.exchange_declare = self._connection_guard(self.exchange_declare)
        self.queue_bind = self._connection_guard(self.queue_bind)
        self.queue_purge = self._connection_guard(self.queue_purge)
        self.__aenter__ = self._connection_guard(self.__aenter__)

        super().__init__(
            packer=packer,
            packer_cls=packer_cls,
            _no_packer=_no_packer,
        )

    def _connection_guard(self, fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                await self.ok()

                return await fn(*args, **kwargs)
            except AioamqpException:
                await self._disconnect()
                raise

        return wrapper

    @staticmethod
    def _get_default_exchange_kwargs():
        return {'type_name': 'direct'}

    def _init_ensure_locks(self):
        self._ensure_queue_lock = asyncio.Lock()
        self._ensure_exchange_lock = asyncio.Lock()
        self._ensure_bind_queue_lock = asyncio.Lock()

    def _init_known(self):
        self._known_queues = set()
        self._known_exchanges = set()
        self._binded_queues = set()

    async def _ensure(
        self,
        *,
        queue_name,
        queue_kwargs,
        exchange_name,
        exchange_kwargs,
        routing_key,
    ):
        await self._ensure_queue(
            queue_name,
            queue_kwargs=queue_kwargs,
        )

        if exchange_name != '':
            await self._ensure_exchange(
                exchange_name,
                exchange_kwargs=exchange_kwargs,
            )

            if routing_key != '':
                await self._ensure_queue_bind(
                    queue_name,
                    exchange_name,
                    routing_key,
                )

    async def _ensure_queue(self, queue_name, queue_kwargs):
        if queue_name in self._known_queues:
            return

        async with self._ensure_queue_lock:
            if queue_name in self._known_queues:
                return

            await self.queue_declare(
                queue_name,
                queue_kwargs=queue_kwargs,
            )

            self._known_queues.add(queue_name)

    async def _ensure_exchange(self, exchange_name, exchange_kwargs):
        if exchange_name in self._known_exchanges:
            return

        async with self._ensure_exchange_lock:
            if exchange_name in self._known_exchanges:
                return

            await self.exchange_declare(
                exchange_name,
                exchange_kwargs=exchange_kwargs,
            )

            self._known_exchanges.add(exchange_name)

    async def _ensure_queue_bind(self, queue_name, exchange_name, routing_key):
        routing_key = routing_key if routing_key else queue_name

        key = (queue_name, exchange_name, routing_key)

        if key in self._binded_queues:
            return

        async with self._ensure_bind_queue_lock:
            if key in self._binded_queues:
                return

            await self.queue_bind(queue_name, exchange_name, routing_key)

            self._binded_queues.add(key)

    async def queue_declare(self, queue_name, queue_kwargs=None):
        if queue_kwargs is None:
            queue_kwargs = {}

        return await self._queue_declare(
            queue_name=queue_name,
            **queue_kwargs,
        )

    async def exchange_declare(self, exchange_name, exchange_kwargs=None):
        if exchange_kwargs is None:
            exchange_kwargs = self._get_default_exchange_kwargs()

        return await self._exchange_declare(
            exchange_name=exchange_name,
            **exchange_kwargs,
        )

    async def queue_bind(self, queue_name, exchange_name, routing_key=''):
        return await self._queue_bind(
            queue_name=queue_name,
            exchange_name=exchange_name,
            routing_key=routing_key if routing_key else queue_name,
        )

    async def queue_delete(self, queue_name):
        return await self._queue_delete(queue_name=queue_name)

    async def publish(
        self,
        payload,
        queue_name,
        *,
        queue_kwargs=None,
        exchange_name='',
        exchange_kwargs=None,
        routing_key='',
        properties=None,
        # set False because of bug https://github.com/Polyconseil/aioamqp/issues/140  # noqa
        mandatory=False,
        immediate=False,
    ):
        if queue_kwargs is None:
            queue_kwargs = {}

        if exchange_kwargs is None:
            exchange_kwargs = self._get_default_exchange_kwargs()

        if self.packer is not None:
            payload = await self.packer.marshal(payload)

        await self._ensure(
            queue_name=queue_name,
            queue_kwargs=queue_kwargs,
            exchange_name=exchange_name,
            exchange_kwargs=exchange_kwargs,
            routing_key=routing_key,
        )

        return await self._basic_publish(
            payload=payload,
            exchange_name=exchange_name,
            routing_key=routing_key if routing_key else queue_name,
            properties=properties,
            mandatory=mandatory,
            immediate=immediate,
        )

    async def queue_purge(self, queue_name, **kwargs):
        return await self._queue_purge(queue_name, **kwargs)

    async def _connect(self):
        if self._connected:
            return

        async with self._connect_lock:
            if not self._connected:
                await super()._connect(self.amqp_url, **self.amqp_kwargs)

    ok = _connect

    async def _disconnect(self):
        self._init_known()

        await super()._disconnect()

    def close(self):
        self._closed = True

    def wait_closed(self):
        assert self._closed, 'Must be closed first'

        return self._disconnect()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        self.close()
        await self.wait_closed()
