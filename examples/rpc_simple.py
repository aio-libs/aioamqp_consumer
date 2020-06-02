import asyncio

from aioamqp_consumer import RpcClient

from rpc_server import square, amqp_url  # isort:skip


async def main():
    async with RpcClient(amqp_url) as client:
        print(await client.call(square(x=2)))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
