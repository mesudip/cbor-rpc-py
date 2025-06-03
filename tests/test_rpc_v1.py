import pytest
import asyncio
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

from cbor_rpc import Pipe, RpcV1, DeferredPromise, SimplePipe

# Example method implementations for testing
async def sleep_method(seconds: float) -> None:
    await asyncio.sleep(seconds)
    return f"Slept for {seconds} seconds"

def add_method(a: int, b: int) -> int:
    return a + b

def multiply_method(a: int, b: int) -> int:
    return a * b

def throw_error_method(message: str) -> None:
    raise Exception(message)

# Method handler for RPC
def method_handler(method: str, args: List[Any]) -> Any:
    if method == "sleep":
        return sleep_method(*args)
    elif method == "add":
        return add_method(*args)
    elif method == "multiply":
        return multiply_method(*args)
    elif method == "throwError":
        return throw_error_method(*args)
    raise Exception(f"Unknown method: {method}")

# Event handler for RPC
async def event_handler(topic: str, message: Any) -> None:
    pass  # No-op for testing

@pytest.fixture
def pipe():
    return SimplePipe()

@pytest.fixture
def rpc(pipe):
    return RpcV1.make_rpc_v1(pipe, "test_id", method_handler, event_handler)

@pytest.mark.asyncio
async def test_get_id(rpc):
    assert rpc.get_id() == "test_id"

@pytest.mark.asyncio
async def test_add_method_success(rpc):
    result = await rpc.call_method("add", 3, 4)
    assert result == 7

@pytest.mark.asyncio
async def test_multiply_method_success(rpc):
    result = await rpc.call_method("multiply", 5, 6)
    assert result == 30

@pytest.mark.asyncio
async def test_sleep_method_success(rpc):
    result = await rpc.call_method("sleep", 0.1)
    assert result == "Slept for 0.1 seconds"

@pytest.mark.asyncio
async def test_throw_error_method(rpc):
    with pytest.raises(Exception) as exc_info:
        await rpc.call_method("throwError", "Test error")
    assert str(exc_info.value) == "Test error"

@pytest.mark.asyncio
async def test_call_method_timeout(rpc, pipe):
    rpc.set_timeout(100)  # Set short timeout
    with pytest.raises(Exception) as exc_info:
        await rpc.call_method("sleep", 1)  # Long sleep to trigger timeout
    assert exc_info.value.args[0]["timeout"] is True
    assert exc_info.value.args[0]["timeoutPeriod"] == 100

@pytest.mark.asyncio
async def test_call_method_unknown_method(rpc):
    with pytest.raises(Exception) as exc_info:
        await rpc.call_method("unknown", 1)
    assert str(exc_info.value) == "Unknown method: unknown"

@pytest.mark.asyncio
async def test_fire_method(rpc):
    await rpc.fire_method("add", 1, 2)
    assert rpc._counter == 1  # Verify message was sent

@pytest.mark.asyncio
async def test_emit_event(rpc):
    await rpc.emit("test_topic", {"data": "test"})
    # No assertion needed; just verify no crash

@pytest.mark.asyncio
async def test_wait_next_event_success(rpc, pipe):
    async def simulate_event():
        await asyncio.sleep(0.1)
        await pipe.write([1, 3, 0, "test_topic", {"data": "test"}])

    task = asyncio.create_task(simulate_event())
    result = await rpc.wait_next_event("test_topic", 1000)
    assert result == {"data": "test"}
    await task

@pytest.mark.asyncio
async def test_wait_next_event_timeout(rpc):
    with pytest.raises(Exception) as exc_info:
        await rpc.wait_next_event("test_topic", 100)
    assert exc_info.value.args[0]["timeout"] is True
    assert exc_info.value.args[0]["timeoutPeriod"] == 100

@pytest.mark.asyncio
async def test_wait_next_event_already_waiting(rpc):
    rpc._waiters["test_topic"] = DeferredPromise(1000)
    with pytest.raises(Exception) as exc_info:
        await rpc.wait_next_event("test_topic")
    assert str(exc_info.value) == "Already waiting for event"

@pytest.mark.asyncio
async def test_invalid_message_format(rpc, pipe):
    await pipe.write([1, 2, 3])  # Invalid message
    await asyncio.sleep(0.1)  # Allow processing
    # No crash means test passes

@pytest.mark.asyncio
async def test_unsupported_version(rpc, pipe):
    await pipe.write([2, 0, 0, "add", [1, 2]])  # Wrong version
    await asyncio.sleep(0.1)  # Allow processing
    # No crash means test passes

@pytest.mark.asyncio
async def test_concurrent_method_calls(rpc):
    tasks = [
        rpc.call_method("add", i, i)
        for i in range(3)
    ]
    results = await asyncio.gather(*tasks)
    assert results == [0, 2, 4]

@pytest.mark.asyncio
async def test_read_only_client(pipe):
    read_only = RpcV1.read_only_client(SimplePipe())
    
    # We need to directly call the handle_method_call method to test it
    with pytest.raises(Exception) as exc_info:
        read_only.handle_method_call("add", [1, 2])
    
    assert str(exc_info.value) == "Client Only Implementation"
