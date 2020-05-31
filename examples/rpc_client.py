import asyncio
import os

import uvloop
from aioamqp_consumer import RpcClient

from rpc_server import method

amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'
N = 10000


async def main():
    payload = b'test'

    client = RpcClient(amqp_url)

    n = 0

    async def go():
        nonlocal n

        fut = await client.call(method(payload))
        assert payload == await fut

        n += 1
        if n % 100 == 0:
            print(n)

    coros = []

    for i in range(N):
        coros.append(go())

    await asyncio.gather(*coros)

    await client.close()


if __name__ == '__main__':
    uvloop.install()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
