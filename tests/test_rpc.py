import asyncio

import pytest
from aioamqp import AioamqpException
from async_timeout import timeout

from aioamqp_consumer import JsonRpcMethod, RpcError, RpcMethod


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
async def test_json_rpc_kwargs(
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
async def test_json_rpc_args(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    @JsonRpcMethod.init(amqp_queue_name)
    async def test_method(x):
        return x ** 2

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method(2))

    assert test_result == 4


@pytest.mark.asyncio
async def test_json_empty_payload(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    test_data = 42

    @JsonRpcMethod.init(amqp_queue_name)
    async def test_method():
        return test_data

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method())

    assert test_result == test_data


@pytest.mark.asyncio
async def test_rpc_none_arg(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    @RpcMethod.init(amqp_queue_name)
    async def test_method(obj):
        assert obj is None

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method(None))

    assert test_result is None


@pytest.mark.asyncio
async def test_rpc_no_result(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        pass

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method())

    assert test_result is None


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
async def test_rpc_empty_payload(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    test_data = b''

    @RpcMethod.init(amqp_queue_name)
    async def test_method(payload):
        assert payload == test_data
        return test_data

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method(test_data))

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
        wait_response=False,
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


@pytest.mark.asyncio
async def test_on_error_shutdown(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    test_data = b'result'

    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        return test_data

    await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    client._transport.close()

    with pytest.raises(AioamqpException):
        await client.wait(test_method())

    await asyncio.sleep(0.2)

    test_result = await client.wait(test_method())

    test_result == test_data


@pytest.mark.asyncio
async def test_rpc_server_down(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    test_data = b'test'

    calls = 0

    @RpcMethod.init(amqp_queue_name)
    async def test_method(payload):
        nonlocal calls

        if not calls:
            server._transport.close()

        calls += 1

        return payload

    server = await rpc_server_close(test_method, amqp_queue_name)

    client = await rpc_client_close()

    test_result = await client.wait(test_method(test_data))

    assert test_result == test_data

    assert calls == 2


@pytest.mark.asyncio
async def test_rpc_marshal_exc(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):
    @RpcMethod.init(amqp_queue_name)
    async def test_method():
        pass

    calls = 0

    async def _unmarshal(obj):
        nonlocal calls
        calls += 1
        raise ValueError

    test_method.packer.unmarshal = _unmarshal

    await rpc_server_close(
        test_method,
        amqp_queue_name,
        marshal_exc=ZeroDivisionError,
    )

    client = await rpc_client_close()

    with pytest.raises(RpcError) as exc_info:
        await client.wait(test_method())

    assert isinstance(exc_info.value.err, ZeroDivisionError)

    assert calls == 1


@pytest.mark.asyncio
async def test_rpc_client_debug(
    rpc_client_close,
    amqp_queue_name,
):

    calls = 0

    @JsonRpcMethod.init(amqp_queue_name)
    async def test_method():
        nonlocal calls
        calls += 1
        return 'debug'

    client = await rpc_client_close(debug=True)

    ret = await client.wait(test_method())

    assert 'debug' == ret

    assert calls == 1


class FatalError(Exception):
    pass


@pytest.mark.asyncio
async def test_rpc_fatal_exceptions(
    rpc_client_close,
    rpc_server_close,
    amqp_queue_name,
):

    @JsonRpcMethod.init(
        amqp_queue_name,
        auto_reject=True,
        fatal_exceptions=(FatalError,),
    )
    async def test_method():
        raise FatalError

    client = await rpc_client_close()

    await rpc_server_close(test_method, amqp_queue_name)

    with pytest.raises(RpcError) as exc_info:
        await client.wait(test_method())

    assert isinstance(exc_info.value.err, FatalError)
