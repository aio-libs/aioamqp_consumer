import asyncio
import logging

from aiorun import run as _run

logger = logging.getLogger(__name__)


def run(*consumers, **kwargs):
    debug = kwargs.pop('debug')
    loop = kwargs.pop('loop', None)

    if loop is None:
        loop = asyncio.get_event_loop()

    kwargs['loop'] = loop

    reloader = None
    if debug:
        import aioreloader
        reloader = aioreloader.start()

        logger.warning('Running in debug mode!!!')

    def shutdown_callback(loop):
        gather = asyncio.gather(*(
            consumer.__aexit__()
            for consumer in consumers
        ), return_exceptions=True)
        loop.run_until_complete(gather)

        if reloader is not None:
            reloader.cancel()

            try:
                loop.run_until_complete(reloader)
            except asyncio.CancelledError:
                pass
            except BaseException as exc:
                logger.exception(exc)

    kwargs['shutdown_callback'] = shutdown_callback

    _run(**kwargs)
