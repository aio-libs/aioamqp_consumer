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
