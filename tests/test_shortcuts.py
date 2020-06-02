import pytest

from aioamqp_consumer import RpcClient, RpcServer, json_rpc


@pytest.mark.asyncio
async def test_shortcuts(amqp_queue_name, amqp_url):
    test_data = ['test']

    @json_rpc(amqp_queue_name)
    async def local_test_method(payload):
        return payload

    server = RpcServer(amqp_url, method=local_test_method)

    client = RpcClient(amqp_url)

    remote_test_method = json_rpc.remote(amqp_queue_name)

    test_result = await client.call(remote_test_method(test_data))

    assert test_result == test_data

    await client.close()

    await server.stop()
