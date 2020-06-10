import abc
import asyncio
import json

from . import settings
from .log import logger


class Packer(abc.ABC):

    def __init__(self):
        self._marshal_is_coro = asyncio.iscoroutinefunction(self._marshal)
        self._unmarshal_is_coro = asyncio.iscoroutinefunction(self._unmarshal)

    async def marshal(self, obj):
        obj = self._marshal(obj)

        if self._marshal_is_coro:
            obj = await obj

        if not isinstance(obj, bytes):
            raise NotImplementedError

        size = len(obj)

        if size >= settings.PAYLOAD_LOG_SIZE:
            msg = 'Packer.marshal returned more %(size)i'
            context = {'size': size}
            logger.warning(msg, context)

        return obj

    async def unmarshal(self, obj):
        obj = self._unmarshal(obj)

        if self._unmarshal_is_coro:
            obj = await obj

        return obj

    @abc.abstractproperty
    def content_type(self):
        pass

    @abc.abstractmethod
    def pack(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def unpack(self, obj):
        pass

    @abc.abstractmethod
    async def _marshal(self, obj):
        pass

    @abc.abstractmethod
    async def _unmarshal(self, obj):
        pass


class RawPacker(Packer):

    none_payload = b'#!n'
    empty_payload = b'#!e'

    @property
    def content_type(self):
        return 'application/octet-stream'

    def pack(self, *args, **kwargs):
        if kwargs:
            raise NotImplementedError

        if args:
            if len(args) != 1:
                raise NotImplementedError

            return args[0]

        return self.empty_payload

    def unpack(self, obj):
        args = []

        if obj != self.empty_payload:
            args.append(obj)

        kwargs = {}

        return args, kwargs

    def _marshal(self, obj):
        if obj is None:
            return self.none_payload

        return obj

    def _unmarshal(self, obj):
        if obj == self.none_payload:
            return None

        return obj


class JsonPacker(Packer):

    ARGS = 'a'
    KWARGS = 'k'

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

    def pack(self, *args, **kwargs):
        payload = {}

        if args:
            payload[self.ARGS] = args

        if kwargs:
            payload[self.KWARGS] = kwargs

        return payload

    def unpack(self, obj):
        args = obj.get(self.ARGS, [])

        kwargs = obj.get(self.KWARGS, {})

        return args, kwargs

    async def _marshal(self, obj):
        obj = await self.loop.run_in_executor(self.executor, self._dumps, obj)
        obj = obj.encode('utf-8')
        return obj

    async def _unmarshal(self, obj):
        obj = obj.decode('utf-8')
        obj = await self.loop.run_in_executor(self.executor, self._loads, obj)
        return obj


class PackerMixin:

    default_packer_cls = None

    def __init__(self, *, packer, packer_cls, _no_packer):
        self.packer = self.get_packer(
            packer=packer,
            packer_cls=packer_cls,
            _no_packer=_no_packer,
        )

    @classmethod
    def get_packer(
        cls,
        *,
        packer,
        packer_cls,
        _no_packer,
    ):
        if _no_packer:
            return

        if packer and packer_cls:
            raise NotImplementedError

        if packer is None:
            if packer_cls is not None:
                packer = packer_cls()
            elif cls.default_packer_cls is not None:
                packer = cls.default_packer_cls()
            else:
                packer = settings.DEFAULT_PACKER_CLS()

        return packer


from . import settings  # noqa isort:skip
