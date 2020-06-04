from aioamqp_consumer import RpcServer, json_rpc

amqp_url = 'amqp://guest:guest@127.0.0.1:5672//'


@json_rpc(queue_name='random_queue')
async def square(*, x):
    ret = x ** 2

    print(x, ret)

    return ret

if __name__ == '__main__':
    RpcServer(amqp_url, method=square).run()
