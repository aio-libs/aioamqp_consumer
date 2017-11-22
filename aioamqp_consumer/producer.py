import asyncio

from .mixins import AMQPMixin


class Producer(AMQPMixin):

    def __init__(
        self,
        amqp_url,
        *,
        amqp_kwargs=None,
        loop=None
    ):
        if amqp_kwargs is None:
            amqp_kwargs = {}

        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        self.amqp_url = amqp_url

        self.amqp_kwargs = amqp_kwargs

        self._connect_lock = asyncio.Lock(loop=self.loop)
        self._ensure_queue_lock = asyncio.Lock(loop=self.loop)
        self._ensure_exchange_lock = asyncio.Lock(loop=self.loop)
        self._ensure_bind_queue_lock = asyncio.Lock(loop=self.loop)

        self._known_queues = set()
        self._known_exchanges = set()
        self._binded_queues = set()

    async def _ensure_queue(self, queue_name, *, queue_kwargs):
        async with self._ensure_queue_lock:
            if queue_name in self._known_queues:
                return

            await self.queue_declare(
                queue_name,
                queue_kwargs=queue_kwargs,
            )

            self._known_queues.add(queue_name)

    async def _ensure_exchange(self, exchange_name, *, exchange_kwargs):
        async with self._ensure_exchange_lock:
            if exchange_name in self._known_exchanges:
                return

            await self.exchange_declare(
                exchange_name,
                exchange_kwargs=exchange_kwargs,
            )

            self._known_exchanges.add(exchange_name)

    async def _ensure_queue_bind(self, queue_name, exchange_name,
                                 routing_key=''):
        async with self._ensure_bind_queue_lock:
            routing_key = routing_key if routing_key else queue_name

            key = (queue_name, exchange_name, routing_key)

            if key in self._binded_queues:
                return

            try:
                await self._connect()
                await self._queue_bind(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key=routing_key if routing_key else queue_name
                )
            except:  # noqa
                await self._disconnect()
                raise

            self._binded_queues.add(key)

    async def _connect(self):
        async with self._connect_lock:
            if not self._connected:
                await super()._connect(self.amqp_url, **self.amqp_kwargs)

    async def queue_declare(self, queue_name, *, queue_kwargs=None):
        if queue_kwargs is None:
            queue_kwargs = {}

        try:
            await self._connect()

            return await self._queue_declare(
                queue_name=queue_name,
                **queue_kwargs
            )
        except:  # noqa
            await self._disconnect()
            raise

    async def exchange_declare(self, exchange_name, *, exchange_kwargs=None):
        if exchange_kwargs is None:
            exchange_kwargs = {}

        try:
            await self._connect()

            return await self._exchange_declare(
                exchange_name=exchange_name,
                **exchange_kwargs
            )
        except:  # noqa
            await self._disconnect()
            raise

    async def publish(
        self,
        payload,

        queue_name,
        exchange_name='',
        routing_key='',

        properties=None,
        # set False because of bug https://github.com/Polyconseil/aioamqp/issues/140  # noqa
        mandatory=False,
        immediate=False,
        *,
        queue_kwargs=None,
        exchange_kwargs=None
    ):
        if queue_kwargs is None:
            queue_kwargs = {}

        if exchange_kwargs is None:
            # Default exchange type is 'DIRECT'
            exchange_kwargs = {
                'type_name': 'direct'
            }

        assert isinstance(payload, bytes)

        try:
            assert not self._closed, 'Cannot publish while closed'

            await self._connect()

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
                        routing_key
                    )

            return await self._basic_publish(
                payload,
                exchange_name=exchange_name,
                routing_key=routing_key if routing_key else queue_name,
                properties=properties,
                mandatory=mandatory,
                immediate=immediate,
            )
        except:  # noqa
            await self._disconnect()
            raise

    async def queue_purge(self, queue_name, **kwargs):
        try:
            assert not self._closed, 'Cannot purge while closed'

            await self._connect()

            await self._queue_purge(queue_name, **kwargs)
        except:  # noqa
            await self._disconnect()
            raise

    async def _disconnect(self):
        self._known_queues = set()
        self._known_exchanges = set()
        self._binded_queues = set()

        await super()._disconnect()

    def close(self):
        self._closed = True

    async def wait_closed(self):
        assert self._closed, 'Must be closed first'

        await self._disconnect()

    async def __aenter__(self):  # noqa
        return self

    async def __aexit__(self, *exc_info):  # noqa
        self.close()
        await self.wait_closed()
