import asyncio
import inspect
import sys
from functools import partial

PY_344 = sys.version_info >= (3, 4, 4)
PY_350 = sys.version_info >= (3, 5, 0)
PY_352 = sys.version_info >= (3, 5, 2)


def create_task(*, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    if PY_352:
        return loop.create_task

    if PY_344:
        return partial(asyncio.ensure_future, loop=loop)

    return partial(getattr(asyncio, 'async'), loop=loop)


def create_future(*, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    if PY_352:
        return loop.create_future()

    return asyncio.Future(loop=loop)


def iscoroutinepartial(fn):
    # http://bugs.python.org/issue23519

    parent = fn

    while fn is not None:
        parent, fn = fn, getattr(parent, 'func', None)

    return asyncio.iscoroutinefunction(parent)


def iscoroutine(fn):
    if PY_350:
        return inspect.isawaitable(fn)

    return iscoroutinepartial(fn)
