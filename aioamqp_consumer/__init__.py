from .consumer import Consumer
from .exceptions import Ack, DeadLetter, DeliveryError, Reject, RpcError
from .packer import JsonPacker, Packer, RawPacker
from .producer import Producer
from .rpc import RpcClient, RpcMethod, RpcServer

__version__ = '0.2.0'


class JsonRpcMethod(RpcMethod):

    default_packer_cls = JsonPacker


__all__ = (
    'Ack',
    'Consumer',
    'DeadLetter',
    'DeliveryError',
    'JsonPacker',
    'JsonRpcMethod',
    'Packer',
    'Producer',
    'RawPacker',
    'Reject',
    'RpcError',
    'RpcClient',
    'RpcMethod',
    'RpcServer',
)
