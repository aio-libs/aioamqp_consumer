aioamqp_consumer
================

:info: Consumer/producer like library built over amqp (aioamqp)

.. image:: https://img.shields.io/travis/aio-libs/aioamqp_consumer.svg
    :target: https://travis-ci.org/aio-libs/aioamqp_consumer

.. image:: https://img.shields.io/pypi/v/aioamqp_consumer.svg
    :target: https://pypi.python.org/pypi/aioamqp_consumer

Installation
------------

.. code-block:: shell

    pip install aioamqp_consumer

Usage
-----

.. code-block:: python

    import asyncio
    from functools import partial

    from aioamqp_consumer import Consumer, Producer


    async def task(payload, options, sleep=0, *, loop):
        await asyncio.sleep(sleep, loop=loop)
        print(payload)


    async def main(*, loop):
        amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'
        amqp_queue = 'your-queue-here'
        queue_kwargs = {
            'durable': True,
        }
        amqp_kwargs = {}  # https://aioamqp.readthedocs.io/en/latest/api.html#aioamqp.connect

        async with Producer(amqp_url, amqp_kwargs=amqp_kwargs, loop=loop) as producer:
            for _ in range(5):
                await producer.publish(
                    b'hello',
                    amqp_queue,
                    queue_kwargs=queue_kwargs,
                )

        consumer = Consumer(
            amqp_url,
            partial(task, loop=loop, sleep=1),
            amqp_queue,
            queue_kwargs=queue_kwargs,
            amqp_kwargs=amqp_kwargs,
            loop=loop,
        )
        await consumer.scale(20)  # scale up to 20 background coroutines
        await consumer.scale(5)  # downscale to 5 background coroutines
        await consumer.join()  # wait for rabbitmq queue is empty and all local messages are processed
        consumer.close()
        await consumer.wait_closed()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop=loop))
    loop.close()
