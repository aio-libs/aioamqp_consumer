import asyncio

from aioamqp_consumer import RpcClient

from rpc_server import square, amqp_url  # isort:skip


async def main():
    async with RpcClient(amqp_url) as client:
        coros = [
            client.call(square(x=i))
            for i in range(100)
        ]

        print(await asyncio.gather(*coros, return_exceptions=True))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
