import asyncio
from typing import Any, List

import pytest

from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.rpc.rpc_v1 import RpcV1


def _noop_handler(method: str, args: List[Any]) -> Any:
    return None


def _noop_event_handler(topic: str, message: Any) -> None:
    pass


def create_client(pipe: EventPipe[Any, Any]) -> RpcV1:
    return RpcV1.make_rpc_v1(pipe, "client", _noop_handler, _noop_event_handler)


@pytest.mark.asyncio
async def test_rpc_call_handle_logging():
    # Setup
    pipe_client, pipe_server = EventPipe.create_inmemory_pair()
    client = create_client(pipe_client)

    # Create call
    handle = client.create_call("test_method", "arg1")
    call_id = handle.id

    logs = []
    handle.on_log(lambda level, msg: logs.append((level, msg)))

    # Simulate server sending logs: [Protocol=2, Sub=0(Log), ID, Level, Content]
    # Protocol 2 ID is streaming
    await pipe_server.write([2, 0, call_id, 3, "Info Log"])
    await pipe_server.write([2, 0, call_id, 1, "Error Log"])

    await asyncio.sleep(0.01)  # Event loop tick

    assert logs == [(3, "Info Log"), (1, "Error Log")]

    await pipe_client.terminate()
    await pipe_server.terminate()


@pytest.mark.asyncio
async def test_rpc_call_handle_progress():
    # Setup
    pipe_client, pipe_server = EventPipe.create_inmemory_pair()
    client = create_client(pipe_client)

    # Create call
    handle = client.create_call("heavy_task")
    call_id = handle.id

    progress = []
    handle.on_progress(lambda val, meta: progress.append((val, meta)))

    # Simulate server sending progress: [Protocol=2, Sub=1(Progress), ID, Value, Metadata]
    await pipe_server.write([2, 1, call_id, 50, "Halfway"])
    await pipe_server.write([2, 1, call_id, 100, "Done"])

    await asyncio.sleep(0.01)

    assert progress == [(50, "Halfway"), (100, "Done")]
    assert handle.get_progress() == 100

    await pipe_client.terminate()
    await pipe_server.terminate()


@pytest.mark.asyncio
async def test_rpc_call_cancellation():
    # Setup
    pipe_client, pipe_server = EventPipe.create_inmemory_pair()
    client = create_client(pipe_client)

    received_msgs = []

    async def capture_server_msgs(data):
        received_msgs.append(data)

    pipe_server.on("data", capture_server_msgs)

    # Create and Cancel
    handle = client.create_call("cancel_me")
    call_id = handle.id

    # Wait for the initial call message to arrive at server
    await asyncio.sleep(0.01)

    handle.cancel()

    await asyncio.sleep(0.01)

    # Verify messages received by server
    # 1. Call: [1, 0, ID, Method, Args]
    # 2. Cancel: [1, 3, ID]

    # Filter for cancel message: Protocol 1, Subprotocol 3
    cancel_msg = next(
        (m for m in received_msgs if isinstance(m, list) and len(m) >= 2 and m[0] == 1 and m[1] == 3), None
    )

    await pipe_client.terminate()
    await pipe_server.terminate()

    assert cancel_msg is not None
    assert cancel_msg[2] == call_id


@pytest.mark.asyncio
async def test_rpc_call_timeout():
    # Setup
    pipe_client, pipe_server = EventPipe.create_inmemory_pair()
    client = create_client(pipe_client)
    client.set_timeout(100)  # 100ms timeout

    # Create call that server never answers (but we write it to keep pipe alive)
    handle = client.create_call("slow_method")

    with pytest.raises(Exception) as excinfo:  # TimedPromise raises Exception on timeout
        await handle.result

    assert "Timeout" in str(excinfo.value)

    await pipe_client.terminate()
    await pipe_server.terminate()


@pytest.mark.asyncio
async def test_rpc_call_result_resolution():
    # Setup
    pipe_client, pipe_server = EventPipe.create_inmemory_pair()
    client = create_client(pipe_client)

    handle = client.create_call("calc")
    call_id = handle.id

    # Simulate Server Response: [1, 2, ID, Success(True), Result]
    # Note: RpcV1 writes are usually async, but creating the task manualy on server pipe
    asyncio.create_task(pipe_server.write([1, 2, call_id, True, 42]))

    result = await handle.result
    assert result == 42

    await pipe_client.terminate()
    await pipe_server.terminate()
