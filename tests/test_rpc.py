import pytest

from aioamqp_consumer import JsonRpcMethod, RpcClient, RpcMethod, RpcServer


@pytest.mark.asyncio
async def test_rpc_smoke(amqp_queue_name, amqp_url):
    test_data = b'test'

    @RpcMethod.init(amqp_queue_name)
    async def test_method(payload):
        return payload

    server = RpcServer(amqp_url, method=test_method)

    client = RpcClient(amqp_url)

    test_result = await client.call(test_method(test_data))

    assert test_result == test_data

    await client.close()

    await server.stop()


@pytest.mark.asyncio
async def test_json_rpc_smoke(amqp_queue_name, amqp_url):
    @JsonRpcMethod.init(amqp_queue_name)
    async def square_method(*, x):
        return x ** 2

    server = RpcServer(amqp_url, method=square_method)

    client = RpcClient(amqp_url)

    test_result = await client.call(square_method(x=2))

    assert test_result == 4

    await client.close()

    await server.stop()


@pytest.mark.asyncio
async def test_rpc_no_payload(amqp_queue_name, amqp_url):
    test_data = b'test'

    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        return test_data

    server = RpcServer(amqp_url, method=test_method)

    client = RpcClient(amqp_url)

    test_result = await client.call(test_method())

    assert test_result == test_data

    await client.close()

    await server.stop()
