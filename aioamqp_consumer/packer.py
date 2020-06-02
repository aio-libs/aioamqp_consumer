import abc
import asyncio
import json


class Packer(abc.ABC):

    ARGS = 'a'
    KWARGS = 'k'
    empty_payload = b''

    def __init__(self):
        self._marshall_is_coro = asyncio.iscoroutinefunction(self._marshall)
        self._unmarshall_is_coro = asyncio.iscoroutinefunction(self._unmarshall)

    async def marshall(self, obj):
        if obj == self.empty_payload:
            return self.empty_payload

        obj = self._marshall(obj)

        if self._marshall_is_coro:
            obj = await obj

        assert isinstance(obj, bytes)

        return obj

    async def unmarshall(self, obj):
        obj = self._unmarshall(obj)

        if self._unmarshall_is_coro:
            obj = await obj

        return obj

    @abc.abstractproperty
    def content_type(self):
        pass

    @abc.abstractmethod
    async def _marshall(self, obj):
        pass

    @abc.abstractmethod
    async def _unmarshall(self, obj):
        pass

    def pack(self, *args, **kwargs):
        payload = {}

        if args:
            payload[self.ARGS] = args

        if kwargs:
            payload[self.KWARGS] = kwargs

        if payload:
            return payload

        return self.empty_payload

    def unpack(self, obj):
        args = obj.get(self.ARGS, [])

        kwargs = obj.get(self.KWARGS, {})

        return args, kwargs


class RawPacker(Packer):

    @property
    def content_type(self):
        return 'application/octet-stream'

    def _marshall(self, obj):
        return obj

    def _unmarshall(self, obj):
        return obj

    def pack(self, *args, **kwargs):
        if args:
            return args[0]

        return self.empty_payload

    def unpack(self, obj):
        args = []

        if obj != self.empty_payload:
            args.append(obj)

        kwargs = {}

        return args, kwargs


class JsonPacker(Packer):

    def __init__(self, *, executor=None):
        self.executor = executor
        self.loop = asyncio.get_event_loop()

        super().__init__()

    @property
    def content_type(self):
        return 'application/json'

    def _dumps(self, obj):
        return json.dumps(obj)

    def _loads(self, obj):
        return json.loads(obj)

    async def _marshall(self, obj):
        obj = await self.loop.run_in_executor(self.executor, self._dumps, obj)
        obj = obj.encode('utf-8')
        return obj

    async def _unmarshall(self, obj):
        obj = obj.decode('utf-8')
        obj = await self.loop.run_in_executor(self.executor, self._loads, obj)
        return obj
