import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor


def _run(*, loop, shutdown_callback, executor_workers):
    def shutdown_handler():
        loop.remove_signal_handler(signal.SIGTERM)
        loop.remove_signal_handler(signal.SIGINT)

        loop.call_soon_threadsafe(loop.stop)

    loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    executor = ThreadPoolExecutor(max_workers=executor_workers)
    loop.set_default_executor(executor)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    try:
        shutdown_callback(loop)
    except BaseException:
        pass

    executor.shutdown(wait=True)

    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


def run(*consumers, loop=None, executor_workers=10):
    if loop is None:
        loop = asyncio.get_event_loop()

    def shutdown_callback(loop):
        gather = asyncio.gather(*(
            consumer.__aexit__()
            for consumer in consumers
        ), return_exceptions=True)
        loop.run_until_complete(gather)

    kwargs = {
        'executor_workers': executor_workers,
        'shutdown_callback': shutdown_callback,
        'loop': loop,
    }

    _run(**kwargs)
