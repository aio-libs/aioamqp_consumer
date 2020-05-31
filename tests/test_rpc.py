import pytest

from aioamqp_consumer import RpcClient, RpcMethod, RpcServer


@pytest.mark.asyncio
async def test_rpc_smoke(amqp_queue_name, amqp_url):
    test_data = b'test'

    @RpcMethod.init(amqp_queue_name)
    async def test_method(payload):
        return payload

    server = RpcServer(amqp_url, test_method)

    client = RpcClient(amqp_url)

    response = await client.call(test_method(test_data))

    test_result = await response

    assert test_result == test_data

    await client.close()

    await server.stop()
