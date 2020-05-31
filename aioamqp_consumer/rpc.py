import asyncio
import uuid
from collections import defaultdict
from functools import partial

from .consumer import Consumer
from .exceptions import DeliveryException, RpcError
from .log import logger
from .producer import Producer

PREFIX = __package__ + '_rpc_'


class RpcClient(Consumer):

    exclusive = True

    def __init__(self, amqp_url, *, tasks_per_worker=3, loop=None):
        kwargs = {
            'queue_kwargs': {
                'exclusive': True,
            },
            'concurrency': 1,
            'tasks_per_worker': tasks_per_worker,
            'loop': loop
        }

        super().__init__(amqp_url, self._on_rpc_callback, '', **kwargs)

        self._map = defaultdict(self.loop.create_future)

        self._ensure_queue_lock = asyncio.Lock()

        self._known_queues = set()

    def join(self):
        raise NotImplementedError

    def _purge_map(self):
        for fut in self._map.values():
            if not fut.done():
                fut.set_exception(asyncio.TimeoutError)

    def _on_error_callback(self, exc):
        self._purge_map()

        self._known_queues = set()

        super()._on_error_callback(exc)

    def _on_rpc_callback(self, payload, properties):
        corr_id = properties.correlation_id

        assert corr_id in self._map

        fut = self._map[corr_id]

        if properties.content_type == RpcError.content_type:
            err = RpcError.loads(payload)

            fut.set_exception(err)
        else:
            fut.set_result(payload)

    def _map_pop(self, fut, *, corr_id):
        self._map.pop(corr_id)

    def _after_connect(self):
        self.queue_name = self._queue_info['queue']

    async def _connect(self):
        self.queue_name = ''

        await super()._connect()

    _ensure_queue = Producer._ensure_queue

    async def queue_declare(self, queue_name, *, queue_kwargs):
        return await self._queue_declare(
            queue_name=queue_name,
            **queue_kwargs,
        )

    async def call(self, rpc_call):
        if not self._connected:
            await self.ok()

        corr_id = str(uuid.uuid1())

        fut = self._map[corr_id]
        fut.add_done_callback(partial(self._map_pop, corr_id=corr_id))

        # TODO: exchange_name, routing_key, other params
        await self._ensure_queue(
            rpc_call.queue_name,
            queue_kwargs={},
        )

        await self._basic_publish(
            payload=rpc_call.payload,
            # TODO: exchange_name, routing_key, other params
            exchange_name='',
            routing_key=rpc_call.queue_name,
            properties={
                'reply_to': self.queue_name,
                'correlation_id': corr_id,
            },
        )

        return asyncio.shield(fut)

    def close(self):
        super().close()

        return self.wait_closed()

    async def __aexit__(self, *exc_info):
        await self.close()


class RpcCall:

    def __init__(self, queue_name, payload):
        self.queue_name = queue_name
        self.payload = payload


class RpcMethod:

    def __init__(self, fn):
        self.fn = fn

        self.queue_name = PREFIX + fn.__name__

    def __call__(self, payload):
        return RpcCall(self.queue_name, payload)

    async def call(self, payload, properties, *, consumer):
        _properties = {
            'correlation_id': properties.correlation_id,
        }

        try:
            ret = await self.fn(payload)
        except asyncio.CancelledError:
            raise
        except DeliveryException:
            raise
        except Exception as exc:
            logger.warning(exc, exc_info=exc)

            ret = RpcError(exc).dumps()
            _properties['content_type'] = RpcError.content_type

        await consumer._basic_publish(
            payload=ret,
            # TODO: exchange_name, routing_key, other params
            exchange_name='',
            routing_key=properties.reply_to,
            properties=_properties,
        )

        return ret


class RpcServer(Consumer):

    exclusive = False

    def __init__(
        self,
        amqp_url,
        method,
        *,
        concurrency=1,
        tasks_per_worker=1,
        loop=None
    ):
        args = (amqp_url, method.call, method.queue_name)

        super().__init__(
            *args,
            concurrency=concurrency,
            tasks_per_worker=tasks_per_worker,
            loop=loop
        )

    def _wrap(self, payload, properties):
        return self.task(payload, properties, consumer=self)

    def stop(self):
        self.close()
        return self.wait_closed()
