import asyncio
import uuid
from collections import defaultdict
from functools import partial

from . import settings
from .consumer import Consumer
from .exceptions import DeliveryError, RpcError
from .log import logger
from .producer import Producer
from .utils import unpartial


class RpcCall:

    ARGS = 'a'
    KWARGS = 'k'

    def __init__(
        self,
        payload,
        queue_name,
        queue_kwargs,
        exchange_name,
        exchange_kwargs,
        routing_key,
        mandatory,
        immediate,
        packer,
    ):
        self.payload = payload
        self.queue_name = queue_name
        self.queue_kwargs = queue_kwargs
        self.exchange_name = exchange_name
        self.exchange_kwargs = exchange_kwargs
        self.routing_key = routing_key
        self.mandatory = mandatory
        self.immediate = immediate
        self.packer = packer

    @property
    def content_type(self):
        return self.packer.content_type

    async def request(self):
        if self.payload == self.empty_payload:
            return self.empty_payload

        return await self.packer.marshall(self.payload)

    async def response(self, fut):
        shield = asyncio.shield(fut)
        shield._log_traceback = False

        payload = await shield

        return await self.packer.unmarshall(payload)

    empty_payload = b''

    @classmethod
    def marshall(cls, *args, **kwargs):
        payload = {}

        if args:
            payload[cls.ARGS] = args

        if kwargs:
            payload[cls.KWARGS] = kwargs

        if payload:
            return payload

        return cls.empty_payload

    @classmethod
    def unmarshall(cls, obj):
        args = obj.get(cls.ARGS, [])

        kwargs = obj.get(cls.KWARGS, {})

        return args, kwargs


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
        err = RpcError(asyncio.TimeoutError())

        for fut in self._map.values():
            if not fut.done():
                fut.set_exception(err)

            fut._log_traceback = False

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
            fut._log_traceback = False
        else:
            fut.set_result(payload)

    def _map_pop(self, fut, *, corr_id):
        self._map.pop(corr_id)

    def _after_connect(self):
        self.queue_name = self._queue_info['queue']

    async def _connect(self):
        self.queue_name = ''

        await super()._connect()

    async def warmup(self, method):
        await self.ok()

        await self._ensure(
            queue_name=method.queue_name,
            queue_kwargs=method.queue_kwargs,
            exchange_name=method.exchange_name,
            exchange_kwargs=method.exchange_kwargs,
            routing_key=method.routing_key,
        )

    async def call(self, rpc_call, *, wait=True):
        payload = await rpc_call.request()

        await self.ok()

        await self._ensure(
            queue_name=rpc_call.queue_name,
            queue_kwargs=rpc_call.queue_kwargs,
            exchange_name=rpc_call.exchange_name,
            exchange_kwargs=rpc_call.exchange_kwargs,
            routing_key=rpc_call.queue_name,
        )

        corr_id = str(uuid.uuid1())

        fut = self._map[corr_id]
        fut._log_traceback = False
        fut.add_done_callback(partial(self._map_pop, corr_id=corr_id))

        properties = {
            'reply_to': self.queue_name,
            'correlation_id': corr_id,
            'content_type': rpc_call.content_type,
        }

        await self._basic_publish(
            payload=payload,
            exchange_name=rpc_call.exchange_name,
            routing_key=rpc_call.queue_name,
            properties=properties,
            mandatory=rpc_call.mandatory,
            immediate=rpc_call.immediate,
        )

        if not wait:
            return

        return await rpc_call.response(fut)

    def close(self):
        super().close()

        return self.wait_closed()

    async def __aexit__(self, *exc_info):
        await self.close()


class RpcMethod:

    mandatory = False

    immediate = False

    default_packer_cls = None

    def __init__(
        self,
        method,
        *,
        queue_name,
        queue_kwargs,
        exchange_name,
        exchange_kwargs,
        routing_key,
        packer,
    ):
        self.method = method
        self.queue_name = queue_name
        self.queue_kwargs = queue_kwargs
        self.exchange_name = exchange_name
        self.exchange_kwargs = exchange_kwargs
        self.routing_key = routing_key
        self.packer = packer

        _fn = unpartial(self.method)
        self._method_is_coro = asyncio.iscoroutinefunction(_fn)

    _get_default_exchange_kwargs = Producer._get_default_exchange_kwargs

    @classmethod
    def init(
        cls,
        queue_name,
        *,
        queue_kwargs=None,
        exchange_name='',
        exchange_kwargs=None,
        routing_key='',
        packer=None,
        packer_cls=None,
    ):
        if queue_kwargs is None:
            queue_kwargs = {}

        if exchange_kwargs is None:
            exchange_kwargs = cls._get_default_exchange_kwargs()

        if packer and packer_cls:
            raise NotImplementedError

        if packer is None:
            if packer_cls is not None:
                packer = packer_cls()

            elif cls.default_packer_cls is not None:
                packer = cls.default_packer_cls()
            else:
                packer = settings.DEFAULT_PACKER_CLS()

        def wrapper(method):
            method = cls(
                method,
                queue_name=queue_name,
                queue_kwargs=queue_kwargs,
                exchange_name=exchange_name,
                exchange_kwargs=exchange_kwargs,
                routing_key=routing_key,
                packer=packer,
            )

            return method

        return wrapper

    def __call__(self, *args, **kwargs):
        payload = RpcCall.marshall(*args, **kwargs)

        return RpcCall(
            payload=payload,
            queue_name=self.queue_name,
            queue_kwargs=self.queue_kwargs,
            exchange_name=self.exchange_name,
            exchange_kwargs=self.exchange_kwargs,
            routing_key=self.routing_key,
            mandatory=self.mandatory,
            immediate=self.immediate,
            packer=self.packer,
        )

    async def call(self, payload, properties, *, amqp_mixin):
        _properties = {'correlation_id': properties.correlation_id}

        try:
            obj = await self.packer.unmarshall(payload)

            args, kwargs = RpcCall.unmarshall(obj)

            ret = self.method(*args, **kwargs)

            if self._method_is_coro:
                ret = await ret
        except asyncio.CancelledError:
            raise
        except DeliveryError:
            raise
        except Exception as exc:
            logger.warning(exc, exc_info=exc)

            ret = RpcError(exc).dumps()
            _properties['content_type'] = RpcError.content_type
        else:
            ret = await self.packer.marshall(ret)
            _properties['content_type'] = self.packer.content_type

        await amqp_mixin._basic_publish(
            payload=ret,
            exchange_name=self.exchange_name,
            routing_key=properties.reply_to,
            properties=_properties,
            mandatory=self.mandatory,
            immediate=self.immediate,
        )


class RpcServer(Consumer):

    def __init__(self, amqp_url, *, method, **kwargs):
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
