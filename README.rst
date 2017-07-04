aioamqp_consumer
================

:info: Consumer/producer like library built over amqp (aioamqp)

.. image:: https://img.shields.io/travis/wikibusiness/aioamqp_consumer.svg
    :target: https://travis-ci.org/wikibusiness/aioamqp_consumer

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


    async def main(loop):
        amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'
        amqp_queue = 'your-queue-here'
        async with Producer(amqp_url, loop=loop) as producer:
            for _ in range(5):
                await producer.publish(b'hello', amqp_queue, durable=True)

        consumer = Consumer(
            amqp_url,
            partial(task, loop=loop, sleep=1),
            amqp_queue,
            loop=loop,
        )
        await consumer.scale(20)
        await consumer.scale(5)
        await consumer.join()
        consumer.close()
        await consumer.wait_closed()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
