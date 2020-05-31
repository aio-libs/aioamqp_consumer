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
