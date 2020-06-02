import asyncio

import uvloop

from aioamqp_consumer import RpcClient

from rpc_server import method  # isort:skip

amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'
N = 10000


async def main():
    payload = b'test'

    async with RpcClient(amqp_url) as client:
        await client.warmup(method)

        gather = req = res = 0

        async def go():
            nonlocal gather, req, res

            if gather % 100 == 0:
                print(gather, 'gather')

            gather += 1

            fut = await client.call(method(payload))

            if req % 100 == 0:
                print(req, 'request')

            req += 1

            assert payload == await fut

            res += 1

            if res % 100 == 0:
                print(res, 'response')

        coros = []

        for i in range(N):
            coros.append(go())

        await asyncio.gather(*coros)


if __name__ == '__main__':
    uvloop.install()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
