import asyncio

from aiorun import run


def serve(*consumers, **kwargs):
    loop = kwargs.pop('loop', None)

    if loop is None:
        loop = asyncio.get_event_loop()

    def shutdown_callback(loop):
        gather = asyncio.gather(*(
            consumer.__aexit__()
            for consumer in consumers
        ))
        loop.run_until_complete(gather)

    kwargs['shutdown_callback'] = shutdown_callback
    kwargs['loop'] = loop

    run(**kwargs)
