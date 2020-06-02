import asyncio

from aioamqp_consumer import JsonRpcMethod, RpcServer

amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'


@JsonRpcMethod.init(queue_name='random_queue')
async def square(*args, **kwargs):
    print(args, kwargs)
    return kwargs['x'] ** 2


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    server = RpcServer(amqp_url, method=square)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(server.stop())
    loop.close()
