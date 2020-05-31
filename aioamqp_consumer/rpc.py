import asyncio
import uuid
from collections import defaultdict
from functools import partial

from .consumer import Consumer
from .exceptions import DeliveryException, RpcError
from .log import logger
from .producer import Producer


class RpcClient(Consumer):

    exclusive = True

    def __init__(self, amqp_url, **kwargs):
        kwargs.setdefault('queue_kwargs', {'exclusive': True})
        kwargs['concurrency'] = 1

        super().__init__(amqp_url, self._on_rpc_callback, '', **kwargs)

        self._map = defaultdict(self.loop.create_future)

        self._init_ensure_locks()
        self._init_known()

    _init_known = Producer._init_known
    _init_ensure_locks = Producer._init_ensure_locks

    _get_default_exchange_kwargs = Producer._get_default_exchange_kwargs

    _ensure = Producer._ensure
    _ensure_queue = Producer._ensure_queue
    _ensure_exchange = Producer._ensure_exchange
    _ensure_queue_bind = Producer._ensure_queue_bind

    queue_declare = Producer.queue_declare
    exchange_declare = Producer.exchange_declare
    queue_bind = Producer.queue_bind

    def join(self):
        raise NotImplementedError

    def _purge_map(self):
        for fut in self._map.values():
            if not fut.done():
                fut.set_exception(asyncio.TimeoutError)

    def _on_error_callback(self, exc):
        self._purge_map()

        self._init_known()

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

    async def call(self, rpc_call):
        if not self._connected:
            await self.ok()

        corr_id = str(uuid.uuid1())

        fut = self._map[corr_id]
        fut.add_done_callback(partial(self._map_pop, corr_id=corr_id))

        await self._ensure(
            queue_name=rpc_call.queue_name,
            queue_kwargs=rpc_call.queue_kwargs,
            exchange_name=rpc_call.exchange_name,
            exchange_kwargs=rpc_call.exchange_kwargs,
            routing_key=rpc_call.queue_name,
        )

        properties = {
            'reply_to': self.queue_name,
            'correlation_id': corr_id,
        }

        await self._basic_publish(
            rpc_call.payload,
            exchange_name=rpc_call.exchange_name,
            routing_key=rpc_call.queue_name,
            properties=properties,
            mandatory=rpc_call.mandatory,
            immediate=rpc_call.immediate,
        )

        return asyncio.shield(fut)

    def close(self):
        super().close()

        return self.wait_closed()

    async def __aexit__(self, *exc_info):
        await self.close()


class RpcCall:

    def __init__(
        self,
        payload,
        *,
        queue_name,
        queue_kwargs,
        exchange_name,
        exchange_kwargs,
        mandatory,
        immediate,
    ):
        self.payload = payload
        self.queue_name = queue_name
        self.queue_kwargs = queue_kwargs
        self.exchange_name = exchange_name
        self.exchange_kwargs = exchange_kwargs
        self.mandatory = mandatory
        self.immediate = immediate


class RpcMethod:

    mandatory = False

    immediate = False

    def __init__(
        self,
        fn,
        *,
        queue_name,
        queue_kwargs,
        exchange_name,
        exchange_kwargs,
    ):
        self.fn = fn
        self._queue_name = queue_name
        self.queue_kwargs = queue_kwargs
        self.exchange_name = exchange_name
        self.exchange_kwargs = exchange_kwargs

    _get_default_exchange_kwargs = Producer._get_default_exchange_kwargs

    @classmethod
    def init(
        cls,
        queue_name,
        *,
        queue_kwargs=None,
        exchange_name='',
        exchange_kwargs=None,
    ):
        if queue_kwargs is None:
            queue_kwargs = {}

        if exchange_kwargs is None:
            exchange_kwargs = cls._get_default_exchange_kwargs()

        def wrapper(fn):
            method = cls(
                fn,
                queue_name=queue_name,
                queue_kwargs=queue_kwargs,
                exchange_name=exchange_name,
                exchange_kwargs=exchange_kwargs,
            )

            return method

        return wrapper

    @property
    def queue_name(self):
        return self._queue_name

    def __call__(self, payload):
        assert isinstance(payload, bytes)

        return RpcCall(
            payload,
            queue_name=self.queue_name,
            queue_kwargs=self.queue_kwargs,
            exchange_name=self.exchange_name,
            exchange_kwargs=self.exchange_kwargs,
            mandatory=self.mandatory,
            immediate=self.immediate,
        )

    async def call(self, payload, properties, *, amqp_mixin):
        _properties = {'correlation_id': properties.correlation_id}

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
        else:
            assert isinstance(ret, bytes)

        await amqp_mixin._basic_publish(
            payload=ret,
            exchange_name=self.exchange_name,
            routing_key=properties.reply_to,
            properties=_properties,
            mandatory=self.mandatory,
            immediate=self.immediate,
        )

        return ret


class RpcServer(Consumer):

    def __init__(self, amqp_url, method, **kwargs):
        kwargs.setdefault('tasks_per_worker', 1)
        args = (amqp_url, method.call, method.queue_name)

        super().__init__(*args, **kwargs)

    def _wrap(self, payload, properties):
        return self.task(
            payload,
            properties,
            amqp_mixin=self,
        )

    def stop(self):
        self.close()
        return self.wait_closed()
