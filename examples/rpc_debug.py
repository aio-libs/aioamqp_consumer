import asyncio

from aioamqp_consumer import RpcClient, rpc

payload = b'test'


@rpc(queue_name='random_queue')
async def method(payload):
    print(payload)
    return payload


async def main():
    amqp_url = 'amqp://guest:guest@non_existing.host:5672//'

    async with RpcClient(amqp_url, debug=True) as client:
        assert payload == await client.wait(method(payload))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
