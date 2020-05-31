import asyncio

import uvloop
from aioamqp_consumer import RpcMethod, RpcServer


@RpcMethod.init(queue_name='random_queue')
async def method(payload):
    return payload


if __name__ == '__main__':
    uvloop.install()

    loop = asyncio.get_event_loop()

    amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'

    server = RpcServer(amqp_url, method)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(server.stop())

    loop.close()
