from .consumer import Consumer
from .exceptions import Ack, DeadLetter, DeliveryError, Reject, RpcError
from .packer import JsonPacker, Packer, RawPacker
from .producer import Producer
from .rpc import RpcClient, RpcMethod, RpcServer
from .run import run

__version__ = '0.3.3'


class JsonConsumer(Consumer):

    default_packer_cls = JsonPacker


class JsonProducer(Producer):

    default_packer_cls = JsonPacker


class JsonRpcMethod(RpcMethod):

    default_packer_cls = JsonPacker


def _rpc(init):
    def wrapper(*args, **kwargs):
        return init(*args, **kwargs)

    return wrapper


rpc = _rpc(RpcMethod.init)
rpc.remote = _rpc(RpcMethod.remote_init)


json_rpc = _rpc(JsonRpcMethod.init)
json_rpc.remote = _rpc(JsonRpcMethod.remote_init)


__all__ = (
    'Ack',
    'Consumer',
    'DeadLetter',
    'DeliveryError',
    'JsonConsumer',
    'JsonPacker',
    'JsonProducer',
    'JsonRpcMethod',
    'Packer',
    'Producer',
    'RawPacker',
    'Reject',
    'RpcClient',
    'RpcError',
    'RpcMethod',
    'RpcServer',
    'json_rpc',
    'rpc',
    'run',
)
