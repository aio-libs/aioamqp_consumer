import pytest

from aioamqp_consumer import RpcClient, RpcMethod, RpcServer


@pytest.mark.asyncio
async def test_rpc(amqp_queue_name, amqp_url, loop):
    class TestRpcMethod(RpcMethod):

        @property
        def queue_name(self):
            return amqp_queue_name

    test_data = b'test'

    @TestRpcMethod
    async def test_method(payload):
        return payload

    server = RpcServer(amqp_url, test_method, loop=loop)

    client = RpcClient(amqp_url, loop=loop)

    response = await client.call(test_method(test_data))

    test_result = await response

    assert test_result == test_data

    await client.close()

    await server.stop()
