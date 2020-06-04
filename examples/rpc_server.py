from aioamqp_consumer import JsonRpcMethod, RpcServer

amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'


@JsonRpcMethod.init(queue_name='random_queue')
async def square(*, x):
    ret = x ** 2

    print(x, ret)

    return ret


if __name__ == '__main__':
    RpcServer(amqp_url, method=square).run()
