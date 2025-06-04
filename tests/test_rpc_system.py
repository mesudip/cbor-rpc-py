import pytest
import asyncio
from typing import Any, Callable, Dict, List, Optional
from cbor_rpc import AbstractEmitter, Pipe, SimplePipe, Duplex, DeferredPromise, RpcV1, RpcV1Server

# Concrete implementation of RpcV1Server for testing
class ConcreteRpcServer(RpcV1Server):
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        if method == "echo":
            return args[0] if args else None
        if method == "async_echo":
            await asyncio.sleep(0.01)
            return args[0] if args else None
        raise Exception(f"Unknown method: {method}")

    async def validate_event_broadcast(self, connection_id: str, topic: str, message: Any) -> bool:
        await asyncio.sleep(0.01)  # Simulate async validation
        return topic != "invalid"

# Mock Pipe for testing
class MockPipe(Pipe):
    def __init__(self):
        super().__init__()
        self.written_data = []
        self.terminated = False
        self.terminate_args = None

    async def write(self, chunk: Any) -> bool:
        self.written_data.append(chunk)
        await self._notify("data", chunk) 
        return True

    async def terminate(self, *args: Any) -> None:
        self.terminated = True
        self.terminate_args = args
        await self._emit("close", *args)

    async def simulate_data(self, data: Any):
        await self._notify("data", data)

    async def simulate_close(self, *args: Any):
        await self._emit("close", *args)

@pytest.mark.asyncio
async def test_simple_pipe():
    pipe = SimplePipe[str]()
    events = []

    async def data_handler(data: str):
        await asyncio.sleep(0.01)
        events.append(data)

    pipe.on("data", data_handler)
    await pipe.write("test_data")
    await asyncio.sleep(0.02)
    assert events == ["test_data"]

    closes = []
    async def close_handler(*args):
        closes.append(args)

    pipe.on("close", close_handler)
    await pipe.terminate(1000, "closed")
    await asyncio.sleep(0.02)
    assert closes == [(1000, "closed")]

@pytest.mark.asyncio
async def test_duplex():
    duplex = Duplex[str, int]()
    reader_data = []
    writer_data = []

    async def reader_handler(data: str):
        await asyncio.sleep(0.01)
        reader_data.append(data)

    async def writer_handler(data: int):
        await asyncio.sleep(0.01)
        writer_data.append(data)

    duplex.reader.on("data", reader_handler)
    duplex.writer.on("data", writer_handler)

    await duplex.write(42)
    await duplex.reader.write("hello")
    await asyncio.sleep(0.02)
    assert writer_data == [42]
    assert reader_data == ["hello"]

    errors = []
    async def error_handler(err: Exception):
        errors.append(str(err))

    duplex.on("error", error_handler)
    await duplex.reader._emit("error", Exception("test error"))
    await asyncio.sleep(0.02)
    assert errors == ["test error"]

@pytest.mark.asyncio
async def test_pipe_attach():
    pipe1 = MockPipe()
    pipe2 = MockPipe()
    Pipe.attach(pipe1, pipe2)

    await pipe1.write("data1")
    await pipe2.write("data2")
    await asyncio.sleep(0.02)
    assert "data1" in pipe2.written_data
    assert "data2" in pipe1.written_data

@pytest.mark.asyncio
async def test_deferred_promise():
    promise = DeferredPromise(100)
    await promise.resolve("success")
    result = await promise.promise
    assert result == "success"

    promise = DeferredPromise(100)
    await promise.reject("error")
    with pytest.raises(Exception, match="error"):
        await promise.promise

    promise = DeferredPromise(50)
    with pytest.raises(Exception, match="Timeout on RPC call"):
        await promise.promise

@pytest.mark.asyncio
async def test_rpc_v1():
    pipe = MockPipe()
    server: RpcV1Server = ConcreteRpcServer()
    await server.add_connection("client1", pipe)

    # Test method call
    await pipe.simulate_data([1, 0, 1, "echo", ["test"]])
    await asyncio.sleep(0.02)
    assert any(item == [1, 2, 1, True, "test"] for item in pipe.written_data)

    # Test async method call
    pipe.written_data.clear()
    await pipe.simulate_data([1, 0, 2, "async_echo", ["async_test"]])
    await asyncio.sleep(0.02)
    assert any(item == [1, 2, 2, True, "async_test"] for item in pipe.written_data)

    # Test fire method (no response)
    pipe.written_data.clear()
    await pipe.simulate_data([1, 1, 3, "echo", ["fire_test"]])
    await asyncio.sleep(0.02)
    # Fire method should not generate a response

    # Test event
    events = []
    async def event_handler(topic: str, message: Any):
        events.append((topic, message))

    client = RpcV1.make_rpc_v1(pipe, "client1", lambda m, a: None, event_handler)
    await pipe.simulate_data([1, 3, 0, "test_topic", "test_message"])
    await asyncio.sleep(0.02)
    assert events == [("test_topic", "test_message")]

    # Test wait_next_event
    promise = client.wait_next_event("test_topic", 100)
    await pipe.simulate_data([1, 3, 0, "test_topic", "waited_message"])
    result = await promise
    assert result == "waited_message"

@pytest.mark.asyncio
async def test_rpc_v1_server():
    server = ConcreteRpcServer()
    server.set_timeout(1000)
    pipe1 = MockPipe()
    pipe2 = MockPipe()
    await server.add_connection("client1", pipe1)
    await server.add_connection("client2", pipe2)

    # Test call_method
    result = await server.call_method("client1", "echo", "hello")
    assert result == "hello"

    # Test async call_method
    result = await server.call_method("client1", "async_echo", "async_hello")
    assert result == "async_hello"

    # Test fire_method
    pipe1.written_data.clear()
    await server.fire_method("client1", "echo", "fire")
    await asyncio.sleep(0.02)
    # Check if any message with the right format was written
    assert any(
        isinstance(item, list) and 
        len(item) == 5 and 
        item[0] == 1 and 
        item[1] == 1 and 
        item[3] == "echo" and 
        item[4] == ["fire"] 
        for item in pipe1.written_data
    )

    # Test emit
    pipe1.written_data.clear()
    await server.emit("client1", "test_topic", "test_message")
    await asyncio.sleep(0.02)
    assert [1, 3, 0, "test_topic", "test_message"] in pipe1.written_data

    # Test broadcast
    pipe1.written_data.clear()
    pipe2.written_data.clear()
    await server.broadcast("broadcast_topic", "broadcast_message")
    await asyncio.sleep(0.02)
    assert [1, 3, 0, "broadcast_topic", "broadcast_message"] in pipe1.written_data
    assert [1, 3, 0, "broadcast_topic", "broadcast_message"] in pipe2.written_data

    # Test event broadcast
    pipe1.written_data.clear()
    pipe2.written_data.clear()
    await pipe1.simulate_data([1, 3, 0, "valid_topic", "valid_message"])
    await asyncio.sleep(0.02)
    assert [1, 3, 0, "valid_topic", "valid_message"] in pipe2.written_data

    # Test invalid event broadcast
    pipe2.written_data.clear()
    await pipe1.simulate_data([1, 3, 0, "invalid", "invalid_message"])
    await asyncio.sleep(0.02)
    # Should not broadcast invalid events
    assert not any("invalid_message" in str(data) for data in pipe2.written_data)

    # Test disconnect
    await server.disconnect("client1", "test_reason")
    await asyncio.sleep(0.02)
    assert not server.is_active("client1")
    assert pipe1.terminated
    assert pipe1.terminate_args == (1000, "test_reason")

    # Test with_client
    called = False
    def action(client: RpcV1):
        nonlocal called
        called = True
        assert client.get_id() == "client2"

    assert server.with_client("client2", action)
    assert called
    assert not server.with_client("client3", action)

@pytest.mark.asyncio
async def test_read_only_client():
    pipe = MockPipe()
    client = RpcV1.read_only_client(pipe)
    client.set_timeout(1000)

    # Test that method calls raise the expected exception
    try:
        client.handle_method_call("echo", ["test"])
        assert False, "Should have raised exception"
    except Exception as e:
        assert str(e) == "Client Only Implementation"

    pipe.written_data.clear()
    await pipe.simulate_data([1, 3, 0, "test_topic", "test_message"])
    await asyncio.sleep(0.02)
    # Events should be dropped (no response)

if __name__ == "__main__":
    pytest.main(["-v", __file__])
