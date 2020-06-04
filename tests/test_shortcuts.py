import pytest

from aioamqp_consumer import json_rpc


@pytest.mark.asyncio
async def test_rpc(rpc_client_close, rpc_server_close, amqp_queue_name):
    test_data = 42

    @json_rpc(amqp_queue_name)
    async def test_method(*, x):
        return x

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method(x=test_data))

    assert test_result == test_data
