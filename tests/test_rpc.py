import asyncio

import pytest
from async_timeout import timeout

from aioamqp_consumer import JsonRpcMethod, RpcMethod


@pytest.mark.asyncio
async def test_rpc(rpc_client_close, rpc_server_close, amqp_queue_name):
    test_data = b'test'

    @RpcMethod.init(amqp_queue_name)
    async def test_method(payload):
        return payload

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method(test_data))

    assert test_result == test_data


@pytest.mark.asyncio
async def test_json_rpc(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    @JsonRpcMethod.init(amqp_queue_name)
    async def test_method(*, x):
        return x ** 2

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method(x=2))

    assert test_result == 4


@pytest.mark.asyncio
async def test_rpc_no_payload(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    test_data = b'test'

    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        return test_data

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method())

    assert test_result == test_data


@pytest.mark.asyncio
async def test_rpc_call(
    rpc_client_factory,
    rpc_server_close,
    amqp_queue_name,
):
    fut = asyncio.Future()

    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        fut.set_result(True)

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_factory()

    resp = await client.call(test_method())

    assert resp is None

    assert not client._map

    await client.close()

    async with timeout(1):
        assert await fut


@pytest.mark.asyncio
async def test_rpc_remote(rpc_client_close, rpc_server_close, amqp_queue_name):
    test_data = b'test'

    @RpcMethod.init(amqp_queue_name)
    async def remote_test_method(payload):
        return payload

    await rpc_server_close(remote_test_method, amqp_queue_name)

    client = await rpc_client_close()

    local_test_method = RpcMethod.remote_init(amqp_queue_name)

    test_result = await client.wait(local_test_method(test_data))

    assert test_result == test_data
