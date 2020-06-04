import asyncio

import pytest
from async_timeout import timeout

from aioamqp_consumer import JsonRpcMethod, RpcMethod, RpcError


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

    async with timeout(0.1):
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


@pytest.mark.asyncio
async def test_rpc_timeout(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    fut = asyncio.Future()

    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        await asyncio.sleep(0.2)
        fut.set_result(True)

    server = await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    with pytest.raises(asyncio.TimeoutError):
        await client.wait(test_method(), timeout=0.1)

    await server.join()

    async with timeout(0.2):
        assert await fut


@pytest.mark.asyncio
async def test_rpc_wait_response(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    fut = asyncio.Future()

    test_result = b'result'

    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        await asyncio.sleep(0.2)
        fut.set_result(test_result)
        return b'result'

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    response = await client.wait(
        test_method(),
        timeout=0.1,
        return_response=True,
    )

    with pytest.raises(asyncio.TimeoutError):
        await response

    await asyncio.sleep(0.15)

    async with timeout(0.2):
        assert await fut == test_result

        assert await response == test_result


@pytest.mark.asyncio
async def test_rpc_error(rpc_client_close, rpc_server_close, amqp_queue_name):
    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        class Error(Exception):
            pass

        raise Error

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    with pytest.raises(RpcError) as exc_info:
        await client.wait(test_method())

    # Can't pickle local object
    assert isinstance(exc_info.value.err, AttributeError)
