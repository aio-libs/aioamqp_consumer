import pickle

from .packer import RawPacker

DEFAULT_PACKER_CLS = RawPacker

PICKLE_ERRORS = (
    NotImplementedError,
    AttributeError,
    TypeError,
    pickle.PickleError,
)

PAYLOAD_LOG_SIZE = 1048576  # 1MB
