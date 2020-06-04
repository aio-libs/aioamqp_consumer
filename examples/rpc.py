import asyncio

from aioamqp_consumer import RpcClient, RpcServer, rpc

payload = b'test'


@rpc(queue_name='random_queue')
async def method(payload):
    print(payload)
    return payload


async def main():
    amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'

    server = RpcServer(amqp_url, method=method)

    client = RpcClient(amqp_url)

    ret = await client.wait(method(payload))

    assert ret == payload

    await client.close()

    await server.stop()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
