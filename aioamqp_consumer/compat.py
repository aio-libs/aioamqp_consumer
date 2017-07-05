import asyncio
import sys

try:
    from asyncio import ensure_future
except ImportError:
    ensure_future = getattr(asyncio, 'async')


PY_350 = sys.version_info >= (3, 5, 0)
PY_352 = sys.version_info >= (3, 5, 2)


def create_future(*, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    if PY_352:
        return loop.create_future()

    return asyncio.Future(loop=loop)


def unpartial(fn):
    while hasattr(fn, 'func'):
        fn = fn.func

    return fn
