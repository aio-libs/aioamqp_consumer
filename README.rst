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

Consume/Producer usage
----------------------

.. code-block:: python

    import asyncio

    from aioamqp_consumer import Consumer, Producer


    async def task(payload, properties):
        await asyncio.sleep(1)
        print(payload)


    async def main():
        amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'
        amqp_queue = 'your-queue-here'
        queue_kwargs = {
            'durable': True,
        }
        amqp_kwargs = {}  # https://aioamqp.readthedocs.io/en/latest/api.html#aioamqp.connect

        async with Producer(amqp_url, amqp_kwargs=amqp_kwargs) as producer:
            for _ in range(5):
                await producer.publish(
                    b'hello',
                    amqp_queue,
                    queue_kwargs=queue_kwargs,
                )

        consumer = Consumer(
            amqp_url,
            task,
            amqp_queue,
            queue_kwargs=queue_kwargs,
            amqp_kwargs=amqp_kwargs,
        )
        await consumer.scale(20)  # scale up to 20 background coroutines
        await consumer.scale(5)  # downscale to 5 background coroutines
        await consumer.join()  # wait for rabbitmq queue is empty and all local messages are processed
        consumer.close()
        await consumer.wait_closed()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

RPC usage
---------

.. code-block:: python

    import asyncio

    from aioamqp_consumer import RpcClient, RpcMethod, RpcServer

    payload = b'test'


    @RpcMethod.init(queue_name='random_queue')
    async def method(payload):
        print(payload)
        return payload


    async def main():
        amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'

        server = RpcServer(amqp_url, method)

        client = RpcClient(amqp_url)

        fut = await client.call(method(payload))
        # `method(payload)` will be executed, awaiting result is optional
        ret = await fut

        assert ret == payload

        await client.close()

        await server.stop()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

Thanks
------

The library was donated by `Ocean S.A. <https://ocean.io/>`_

Thanks to the company for contribution.
