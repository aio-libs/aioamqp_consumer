import abc
import asyncio
import json


class Packer(abc.ABC):

    def __init__(self, *, executor=None):
        self.executor = executor
        self.loop = asyncio.get_event_loop()

        self._marshall_is_coro = asyncio.iscoroutinefunction(self._marshall)
        self._unmarshall_is_coro = asyncio.iscoroutinefunction(self._unmarshall)

    async def marshall(self, obj):
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


class RawPacker(Packer):

    @property
    def content_type(self):
        return 'application/octet-stream'

    def _marshall(self, obj):
        return obj

    def _unmarshall(self, obj):
        return obj


class JsonPacker(Packer):

    @property
    def content_type(self):
        return 'application/json'

    async def _marshall(self, obj):
        obj = await self.loop.run_in_executor(self.executor, json.dumps, obj)
        obj = obj.encode('utf-8')
        return obj

    async def _unmarshall(self, obj):
        obj = obj.decode('utf-8')
        obj = await self.loop.run_in_executor(self.executor, json.loads, obj)
        return obj
