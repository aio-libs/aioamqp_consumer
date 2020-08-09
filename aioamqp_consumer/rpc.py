import asyncio
import logging
import uuid
from collections import defaultdict
from functools import partial, partialmethod

import async_timeout
from aioamqp import AioamqpException

from .consumer import Consumer
from .exceptions import DeliveryError, Reject, RpcError
from .log import logger
from .packer import PackerMixin
from .producer import Producer
from .utils import unpartial


class RpcResponse:

    def __init__(self, fut, *, packer, timeout):
        self.fut = fut
        self.packer = packer
        self.timeout = timeout

    def __await__(self):
        return self.response().__await__()

    async def response(self):
        shield = asyncio.shield(self.fut)
        shield._log_traceback = False

        async with async_timeout.timeout(timeout=self.timeout):
            payload = await shield

        return await self.packer.unmarshal(payload)


class RpcCall:

    def __init__(
        self,
        *,
        args,
        kwargs,
        queue_name,
        queue_kwargs,
        exchange_name,
        exchange_kwargs,
        routing_key,
        mandatory,
        immediate,
        packer,
        method,
        _method_is_coro,
    ):
        self.args = args
        self.kwargs = kwargs
        self.queue_name = queue_name
        self.queue_kwargs = queue_kwargs
        self.exchange_name = exchange_name
        self.exchange_kwargs = exchange_kwargs
        self.routing_key = routing_key
        self.mandatory = mandatory
        self.immediate = immediate
        self.packer = packer
        self.method = method
        self._method_is_coro = _method_is_coro

    @property
    def content_type(self):
        return self.packer.content_type

    async def request(self):
        payload = self.packer.pack(*self.args, **self.kwargs)

        return await self.packer.marshal(payload)

    def response(self, fut, *, timeout):
        return RpcResponse(fut, packer=self.packer, timeout=timeout)

    async def _debug_response(self):
        request = await self.request()

        obj = await self.packer.unmarshal(request)

        args, kwargs = self.packer.unpack(obj)

        ret = self.method(*args, **kwargs)

        if self._method_is_coro:
            ret = await ret

        payload = await self.packer.marshal(ret)

        return await self.packer.unmarshal(payload)


class RpcClient(Consumer):

    exclusive = True

    def __init__(self, amqp_url, **kwargs):
        kwargs['queue_kwargs'] = {'exclusive': True}
        kwargs['concurrency'] = 1
        kwargs['_no_packer'] = True

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

        if corr_id in self._map:
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

        if not self.debug:
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

    async def ok(self, timeout=None):
        if not self.debug:
            return await super().ok(timeout=timeout)

    async def call(
        self,
        rpc_call,
        *,
        wait=False,
        wait_response=False,
        timeout=None,
    ):
        if self.debug:
            return await rpc_call._debug_response()

        if not wait and wait_response:
            raise NotImplementedError

        await self.ok()

        await self._ensure(
            queue_name=rpc_call.queue_name,
            queue_kwargs=rpc_call.queue_kwargs,
            exchange_name=rpc_call.exchange_name,
            exchange_kwargs=rpc_call.exchange_kwargs,
            routing_key=rpc_call.queue_name,
        )

        payload = await rpc_call.request()

        _properties = {'content_type': rpc_call.content_type}

        if wait:
            corr_id = str(uuid.uuid1())

            fut = self._map[corr_id]
            fut._log_traceback = False
            fut.add_done_callback(partial(self._map_pop, corr_id=corr_id))

            _properties['reply_to'] = self.queue_name
            _properties['correlation_id'] = corr_id

        try:
            await self._basic_publish(
                payload=payload,
                exchange_name=rpc_call.exchange_name,
                routing_key=rpc_call.queue_name,
                properties=_properties,
                mandatory=rpc_call.mandatory,
                immediate=rpc_call.immediate,
            )
        except AioamqpException:
            if wait:
                if corr_id in self._map:
                    self._map.pop(corr_id, None)

            raise

        if wait:
            response = rpc_call.response(fut, timeout=timeout)

            if wait_response:
                response = await response

            return response

    wait = partialmethod(call, wait=True, wait_response=True)

    def close(self):
        super().close()

        return self.wait_closed()

    async def __aexit__(self, *exc_info):
        await self.close()


class RpcMethod(PackerMixin):

    mandatory = False

    immediate = False

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
        auto_reject,
        auto_reject_delay,
    ):
        self.method = method
        self.queue_name = queue_name
        self.queue_kwargs = queue_kwargs
        self.exchange_name = exchange_name
        self.exchange_kwargs = exchange_kwargs
        self.routing_key = routing_key
        self.packer = packer
        self.auto_reject = auto_reject
        self.auto_reject_delay = auto_reject_delay

        _fn = unpartial(self.method)
        self._method_is_coro = asyncio.iscoroutinefunction(_fn)

    _get_default_exchange_kwargs = Producer._get_default_exchange_kwargs

    @classmethod
    def remote_init(cls, *args, **kwargs):
        return cls.init(*args, **kwargs)(None)

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
        auto_reject=False,
        auto_reject_delay=None,
    ):
        if queue_kwargs is None:
            queue_kwargs = {}

        if exchange_kwargs is None:
            exchange_kwargs = cls._get_default_exchange_kwargs()

        packer = cls.get_packer(
            packer=packer,
            packer_cls=packer_cls,
            _no_packer=False,
        )

        def wrapper(method):
            method = cls(
                method,
                queue_name=queue_name,
                queue_kwargs=queue_kwargs,
                exchange_name=exchange_name,
                exchange_kwargs=exchange_kwargs,
                routing_key=routing_key,
                packer=packer,
                auto_reject=auto_reject,
                auto_reject_delay=auto_reject_delay,
            )

            return method

        return wrapper

    def __call__(self, *args, **kwargs):
        return RpcCall(
            args=args,
            kwargs=kwargs,
            queue_name=self.queue_name,
            queue_kwargs=self.queue_kwargs,
            exchange_name=self.exchange_name,
            exchange_kwargs=self.exchange_kwargs,
            routing_key=self.routing_key,
            mandatory=self.mandatory,
            immediate=self.immediate,
            packer=self.packer,
            method=self.method,
            _method_is_coro=self._method_is_coro,
        )

    async def call(self, payload, properties, *, amqp_mixin):
        _properties = {}

        try:
            if properties.content_type != self.packer.content_type:
                raise NotImplementedError

            try:
                obj = await self.packer.unmarshal(payload)

                args, kwargs = self.packer.unpack(obj)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                amqp_mixin._log_task(
                    'unmarshal error',
                    logging.DEBUG,
                    exc_info=exc,
                )

                if amqp_mixin.marshal_exc is None:
                    raise

                raise amqp_mixin.marshal_exc from exc

            ret = self.method(*args, **kwargs)

            if self._method_is_coro:
                ret = await ret

            try:
                payload = await self.packer.marshal(ret)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                amqp_mixin._log_task(
                    'marshal error',
                    logging.DEBUG,
                    exc_info=exc,
                )

                if amqp_mixin.marshal_exc is None:
                    raise

                raise amqp_mixin.marshal_exc from exc

            _properties['content_type'] = self.packer.content_type
        except asyncio.CancelledError:
            raise
        except DeliveryError:
            raise
        except Exception as exc:
            logger.warning(exc, exc_info=exc)

            if self.auto_reject:
                if self.auto_reject_delay is not None:
                    await asyncio.sleep(self.auto_reject_delay)

                raise Reject from exc

            payload = RpcError(exc).dumps()
            _properties['content_type'] = RpcError.content_type

        if properties.reply_to is not None:
            _properties['correlation_id'] = properties.correlation_id

            await amqp_mixin._basic_publish(
                payload=payload,
                exchange_name=self.exchange_name,
                routing_key=properties.reply_to,
                properties=_properties,
                mandatory=self.mandatory,
                immediate=self.immediate,
            )


class RpcServer(Consumer):

    def __init__(self, amqp_url, *, method, **kwargs):
        kwargs['_no_packer'] = True

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

    async def __aexit__(self, *exc_info):
        await self.stop()
