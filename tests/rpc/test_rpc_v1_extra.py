import asyncio
from typing import Any, List

import pytest

from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.rpc.context import RpcCallContext
from cbor_rpc.rpc.rpc_v1 import RpcV1, RpcCore
from cbor_rpc.rpc.rpc_server import RpcV1Server


class TestRpcServer(RpcV1Server):
    async def handle_method_call(
        self,
        connection_id: str,
        context: RpcCallContext,
        method: str,
        args: List[Any],
    ) -> Any:
        if method == "ping":
            return f"pong:{connection_id}:{args[0]}"
        raise Exception("Unknown method")


def _noop_handler(context: RpcCallContext, method: str, args: List[Any]) -> Any:
    if method == "fire":
        return None
    return "ok"


class CoreOnlyRpc(RpcCore):
    def get_id(self) -> str:
        return "core"

    def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
        if method == "boom":
            raise Exception("boom")
        if method == "nested":
            async def inner() -> str:
                return "ok"

            async def outer():
                return inner()

            return outer()
        return "ok"


@pytest.mark.asyncio
async def test_rpc_v1_proto_validation_and_logging(caplog):
    import logging
    caplog.set_level(logging.INFO)
    pipe_a, pipe_b = EventPipe.create_inmemory_pair()
    rpc = RpcV1.make_rpc_v1(pipe_a, "id", _noop_handler)

    await pipe_b.write([1, 0, 1])
    await pipe_b.write([1, 2, 1])
    await pipe_b.write([1, 9, 1, "x", []])
    await pipe_b.write([1, 0, 999, "ok"])
    await pipe_b.write([2, 1, 2])
    await pipe_b.write([2, 0, 3])
    await pipe_b.write([2, 99, 1, 2, "content"])
    await pipe_b.write([3, 0])
    await pipe_b.write([3, 1, "topic", "msg"])

    await asyncio.sleep(0.05)

    assert rpc._peer_log_level == 3
    
    logs = [r.message for r in caplog.records]
    assert any("Invalid response format" in log for log in logs)
    assert any("Invalid call format" in log for log in logs)
    assert any("Unknown sub-protocol" in log for log in logs)
    assert any("expired request id" in log for log in logs)
    assert any("Invalid format" in log for log in logs)
    assert any("[RemoteLog:LEVEL-99]" in log for log in logs)
    assert any("Invalid event format" in log for log in logs)

    await pipe_a.terminate()
    await pipe_b.terminate()


@pytest.mark.asyncio
async def test_rpc_v1_send_log_filters_by_peer_level():
    pipe_a, pipe_b = EventPipe.create_inmemory_pair()
    rpc = RpcV1.make_rpc_v1(pipe_a, "id", _noop_handler)

    received: List[List[Any]] = []

    async def on_data(data: Any) -> None:
        if isinstance(data, list) and data and data[0] == 2:
            received.append(data)

    pipe_b.pipeline("data", on_data)

    rpc.logger.log("skip")
    await asyncio.sleep(0.01)
    assert received == []

    rpc._peer_log_level = 2
    rpc.logger.debug("skip")
    await asyncio.sleep(0.01)
    assert received == []

    rpc.logger.warn("keep")
    await asyncio.sleep(0.01)
    assert received[-1][1] == 2

    await pipe_a.terminate()
    await pipe_b.terminate()


@pytest.mark.asyncio
async def test_rpc_v1_server_connection_lifecycle():
    server = TestRpcServer()
    pipe_a, pipe_b = EventPipe.create_inmemory_pair()

    client_called = asyncio.Event()

    def client_handler(context: RpcCallContext, method: str, args: List[Any]) -> Any:
        if method == "ping":
            return f"client:{args[0]}"
        if method == "fire":
            client_called.set()
            return None
        raise Exception("Unknown method")

    client = RpcV1.make_rpc_v1(pipe_b, "client", client_handler)

    await server.add_connection("c1", pipe_a)
    assert server.is_active("c1")

    result = await server.call_method("c1", "ping", "hi")
    assert result == "client:hi"

    await server.fire_method("c1", "fire")
    await asyncio.wait_for(client_called.wait(), timeout=1)

    called: List[bool] = []

    def action(_client: Any) -> None:
        called.append(True)

    assert server.with_client("c1", action) is True
    assert called == [True]

    await server.disconnect("c1", "bye")
    assert server.is_active("c1") is False
    assert server.with_client("missing", action) is False

    await pipe_a.terminate()
    await pipe_b.terminate()


@pytest.mark.asyncio
async def test_rpc_v1_server_inactive_client_errors():
    server = TestRpcServer()

    with pytest.raises(Exception) as exc_info:
        await server.call_method("missing", "ping")
    assert str(exc_info.value) == "Client is not active"

    with pytest.raises(Exception) as exc_info:
        await server.fire_method("missing", "ping")
    assert str(exc_info.value) == "Client is not active"


@pytest.mark.asyncio
async def test_rpc_core_fire_error_and_unsupported_event(caplog):
    pipe_a, pipe_b = EventPipe.create_inmemory_pair()
    _rpc = CoreOnlyRpc(pipe_a)

    await pipe_b.write([3, 0, "topic", "msg"])
    await pipe_b.write([1, 3, 1, "boom", []])
    await asyncio.sleep(0.05)

    logs = [r.message for r in caplog.records]
    assert any("Unsupported event message" in log for log in logs)
    assert any("Fired method error" in log for log in logs)

    await pipe_a.terminate()
    await pipe_b.terminate()


@pytest.mark.asyncio
async def test_rpc_core_nested_result_and_error_response():
    pipe_a, pipe_b = EventPipe.create_inmemory_pair()
    _rpc = CoreOnlyRpc(pipe_a)

    responses: List[List[Any]] = []

    async def on_data(data: Any) -> None:
        if isinstance(data, list) and data and data[0] == 1 and data[1] in (0, 1):
            responses.append(data)

    pipe_b.pipeline("data", on_data)

    await pipe_b.write([1, 2, 1, "nested", []])
    await pipe_b.write([1, 2, 2, "boom", []])
    await asyncio.sleep(0.05)

    assert [1, 0, 1, "ok"] in responses
    assert [1, 1, 2, "boom"] in responses

    await pipe_a.terminate()
    await pipe_b.terminate()


@pytest.mark.asyncio
async def test_rpc_v1_server_close_cleanup_and_timeout_applied():
    server = TestRpcServer()
    server.set_timeout(123)
    pipe_a, _pipe_b = EventPipe.create_inmemory_pair()
    await server.add_connection("c1", pipe_a)
    assert server.rpc_clients["c1"]._timeout == 123

    await pipe_a.terminate("done")
    await asyncio.sleep(0.01)
    assert server.is_active("c1") is False


def test_rpc_v1_server_get_client_and_disconnect_missing():
    server = TestRpcServer()
    assert server.get_client("missing") is None
