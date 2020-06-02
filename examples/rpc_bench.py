import asyncio

from rpc_server import amqp_url, square

from aioamqp_consumer import RpcClient

N = 10000


async def main():
    async with RpcClient(amqp_url) as client:
        await client.warmup(square)

        gather = res = 0

        async def go(x):
            nonlocal gather, res

            if gather % 100 == 0:
                print(gather, 'gather')

            gather += 1

            await client.wait(square(x=x))

            res += 1

            if res % 100 == 0:
                print(res, 'response')

        coros = []

        for x in range(N):
            coros.append(go(x))

        await asyncio.gather(*coros)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
