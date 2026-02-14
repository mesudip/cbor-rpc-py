import asyncio
from typing import Any, List

import pytest

from cbor_rpc import EventPipe, RpcV1, TimedPromise
from cbor_rpc.rpc.context import RpcCallContext
from tests.helpers import SimplePipe


async def sleep_method(seconds: float) -> None:
    await asyncio.sleep(seconds)
    return f"Slept for {seconds} seconds"


def add_method(a: int, b: int) -> int:
    return a + b


def multiply_method(a: int, b: int) -> int:
    return a * b


def throw_error_method(message: str) -> None:
    raise Exception(message)


def method_handler(context: RpcCallContext, method: str, args: List[Any]) -> Any:
    if method == "sleep":
        return sleep_method(*args)
    if method == "add":
        return add_method(*args)
    if method == "multiply":
        return multiply_method(*args)
    if method == "throwError":
        return throw_error_method(*args)
    raise Exception(f"Unknown method: {method}")


class EventRpcHelper(RpcV1):
    def get_id(self) -> str:
        return "event_rpc"

    def handle_method_call(self, method: str, args: List[Any]) -> Any:
        raise Exception("Event-only RPC")

    async def on_event(self, topic: str, payload: Any) -> None:
        pass


@pytest.fixture
def pipe():
    return SimplePipe()


@pytest.fixture
def event_rpc(pipe):
    return EventRpcHelper(pipe)


@pytest.fixture
def rpc(pipe):
    return RpcV1.make_rpc_v1(pipe, "test_id", method_handler)


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
    rpc.set_timeout(100)
    with pytest.raises(Exception) as exc_info:
        await rpc.call_method("sleep", 1)
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
    assert rpc._counter == 1


@pytest.mark.asyncio
async def test_emit_event(event_rpc):
    await event_rpc.emit("test_topic", {"data": "test"})


@pytest.mark.asyncio
async def test_wait_next_event_success(event_rpc, pipe):
    async def simulate_event():
        await asyncio.sleep(0.1)
        await pipe.write([3, 0, "test_topic", {"data": "test"}])

    task = asyncio.create_task(simulate_event())
    result = await event_rpc.wait_next_event("test_topic", 1000)
    assert result == {"data": "test"}
    await task


@pytest.mark.asyncio
async def test_wait_next_event_timeout(event_rpc):
    with pytest.raises(Exception) as exc_info:
        await event_rpc.wait_next_event("test_topic", 100)
    assert exc_info.value.args[0]["timeout"] is True
    assert exc_info.value.args[0]["timeoutPeriod"] == 100


@pytest.mark.asyncio
async def test_wait_next_event_already_waiting(event_rpc):
    event_rpc._waiters["test_topic"] = TimedPromise(1000)
    with pytest.raises(Exception) as exc_info:
        await event_rpc.wait_next_event("test_topic")
    assert str(exc_info.value) == "Already waiting for event"


@pytest.mark.asyncio
async def test_invalid_message_format(rpc, pipe):
    await pipe.write([1])
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_unsupported_protocol(rpc, pipe):
    await pipe.write([99, 0, 0, "add", [1, 2]])
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_concurrent_method_calls(rpc):
    tasks = [rpc.call_method("add", i, i) for i in range(3)]
    results = await asyncio.gather(*tasks)
    assert results == [0, 2, 4]


@pytest.mark.asyncio
async def test_read_only_client(pipe):
    read_only = RpcV1.read_only_client(SimplePipe())

    with pytest.raises(Exception) as exc_info:
        read_only.handle_method_call("add", [1, 2])

    assert str(exc_info.value) == "Client Only Implementation"
