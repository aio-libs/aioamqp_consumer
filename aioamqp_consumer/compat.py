import asyncio

try:
    from asyncio import ensure_future
except ImportError:
    ensure_future = getattr(asyncio, 'async')


def create_future(*, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)
