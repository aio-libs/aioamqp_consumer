import asyncio

from rpc_server import amqp_url, square

from aioamqp_consumer import RpcClient


async def main():
    async with RpcClient(amqp_url) as client:
        print(await client.wait(square(x=2)))

        coros = [
            client.wait(square(x=i))
            for i in range(10)
        ]
        print(await asyncio.gather(*coros))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
