"""
Generic parameterized tests for RPC functionality that can run over any backend.
"""

import pytest
import asyncio
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, AsyncGenerator
from dataclasses import dataclass

from cbor_rpc import (
    Pipe, SimplePipe, Duplex, TcpDuplex, TcpServer,
    RpcV1, RpcV1Server, RpcClient, RpcAuthorizedClient
)


# ===== Backend Configuration Classes =====

@dataclass
class RpcBackendConfig:
    """Base class for RPC backend configurations."""
    name: str
    
    @staticmethod
    async def create_server_client_pair() -> Tuple[RpcV1Server, RpcV1]:
        """Create a server and client pair for testing."""
        raise NotImplementedError("Subclasses must implement this method")


class InMemoryRpcBackend(RpcBackendConfig):
    """In-memory RPC backend using SimplePipe."""
    
    def __init__(self):
        super().__init__(name="in-memory")
    
    @staticmethod
    async def create_server_client_pair() -> Tuple[RpcV1Server, RpcV1]:
        # Create server
        server = TestRpcServer()
        
        # Create pipes for bidirectional communication
        client_to_server = SimplePipe()
        server_to_client = SimplePipe()
        
        # Connect the pipes
        Pipe.attach(client_to_server, server_to_client)
        
        # Create client pipe (duplex)
        client_pipe = Duplex()
        client_pipe.reader = server_to_client
        client_pipe.writer = client_to_server
        
        # Create server pipe (duplex)
        server_pipe = Duplex()
        server_pipe.reader = client_to_server
        server_pipe.writer = server_to_client
        
        # Add connection to server
        client_id = str(uuid.uuid4())
        await server.add_connection(client_id, server_pipe)
        
        # Create client
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            lambda m, a: None,  # Client doesn't handle methods in this test
            lambda t, m: None   # Client doesn't handle events in this test
        )
        
        return server, client


class TcpRpcBackend(RpcBackendConfig):
    """TCP-based RPC backend using TcpDuplex."""
    
    def __init__(self):
        super().__init__(name="tcp")
    
    @staticmethod
    async def create_server_client_pair() -> Tuple[RpcV1Server, RpcV1]:
        # Create server
        server = TestRpcServer()
        
        # Create TCP server
        tcp_server = await TcpServer.create('127.0.0.1', 0)
        host, port = tcp_server.get_address()
        
        # Set up connection handler
        client_ready = asyncio.Event()
        client_id = str(uuid.uuid4())
        
        async def on_connection(tcp_duplex: TcpDuplex):
            await server.add_connection(client_id, tcp_duplex)
            client_ready.set()
        
        tcp_server.on_connection(on_connection)
        
        # Create client connection
        client_pipe = await TcpDuplex.create_connection(host, port)
        
        # Wait for server to register the connection
        await client_ready.wait()
        
        # Create client
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            lambda m, a: None,  # Client doesn't handle methods in this test
            lambda t, m: None   # Client doesn't handle events in this test
        )
        
        # Store TCP server for cleanup
        server._tcp_server = tcp_server
        
        return server, client


# ===== Test RPC Server Implementation =====

class TestRpcServer(RpcV1Server):
    """Test RPC server implementation."""
    
    def __init__(self):
        super().__init__()
        self._tcp_server = None
        self._method_handlers = {}
        self._event_validators = {}
    
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle RPC method calls."""
        if method in self._method_handlers:
            return await self._method_handlers[method](connection_id, *args)
        
        # Default handlers
        if method == "echo":
            return args[0] if args else None
        elif method == "add":
            return sum(args)
        elif method == "multiply":
            result = 1
            for arg in args:
                result *= arg
            return result
        elif method == "get_connection_id":
            return connection_id
        elif method == "sleep":
            await asyncio.sleep(args[0])
            return f"Slept for {args[0]} seconds"
        elif method == "error":
            raise Exception(args[0] if args else "Test error")
        else:
            raise Exception(f"Unknown method: {method}")
    
    async def validate_event_broadcast(self, connection_id: str, topic: str, message: Any) -> bool:
        """Validate whether an event should be broadcasted."""
        if topic in self._event_validators:
            return await self._event_validators[topic](connection_id, message)
        
        # Default validators
        if topic == "blocked_topic":
            return False
        return True
    
    def register_method_handler(self, method: str, handler: Callable):
        """Register a custom method handler."""
        self._method_handlers[method] = handler
    
    def register_event_validator(self, topic: str, validator: Callable):
        """Register a custom event validator."""
        self._event_validators[topic] = validator
    
    async def cleanup(self):
        """Clean up resources."""
        # Close all connections
        for conn_id in list(self.active_connections.keys()):
            await self.disconnect(conn_id)
        
        # Close TCP server if it exists
        if self._tcp_server:
            await self._tcp_server.close()


# ===== Test Fixtures =====

@pytest.fixture(params=[
    InMemoryRpcBackend(),
    TcpRpcBackend()
])
async def rpc_backend(request) -> AsyncGenerator[Tuple[RpcV1Server, RpcV1], None]:
    """
    Parameterized fixture that provides different RPC backend implementations.
    
    Returns a tuple of (server, client) for each backend type.
    """
    backend: RpcBackendConfig = request.param
    
    # Print which backend is being tested
    print(f"\nTesting with {backend.name} backend")
    
    # Create server and client
    server, client = await backend.create_server_client_pair()
    
    try:
        yield server, client
    finally:
        # Clean up
        await server.cleanup()


# ===== Generic RPC Tests =====

@pytest.mark.asyncio
async def test_rpc_echo(rpc_backend):
    """Test basic echo functionality across different backends."""
    server, client = rpc_backend
    
    # Test with different data types
    test_values = [
        "hello world",
        123,
        3.14,
        True,
        None,
        [1, 2, 3],
        {"key": "value"},
        {"nested": {"data": [1, 2, {"x": "y"}]}}
    ]
    
    for value in test_values:
        result = await client.call_method("echo", value)
        assert result == value


@pytest.mark.asyncio
async def test_rpc_math_operations(rpc_backend):
    """Test mathematical operations across different backends."""
    server, client = rpc_backend
    
    # Test addition
    result = await client.call_method("add", 1, 2, 3, 4, 5)
    assert result == 15
    
    # Test multiplication
    result = await client.call_method("multiply", 2, 3, 4)
    assert result == 24


@pytest.mark.asyncio
async def test_rpc_connection_id(rpc_backend):
    """Test that connection IDs are properly maintained."""
    server, client = rpc_backend
    
    # Get connection ID from server
    conn_id = await client.call_method("get_connection_id")
    
    # Verify it matches the client's ID
    assert conn_id == client.get_id()
    
    # Verify the connection is active on the server
    assert server.is_active(conn_id)


@pytest.mark.asyncio
async def test_rpc_error_handling(rpc_backend):
    """Test error handling across different backends."""
    server, client = rpc_backend
    
    # Test method that raises an exception
    with pytest.raises(Exception) as exc_info:
        await client.call_method("error", "Custom error message")
    
    assert "Custom error message" in str(exc_info.value)
    
    # Test calling non-existent method
    with pytest.raises(Exception) as exc_info:
        await client.call_method("non_existent_method")
    
    assert "Unknown method" in str(exc_info.value)


@pytest.mark.asyncio
async def test_rpc_timeout(rpc_backend):
    """Test RPC timeout functionality."""
    server, client = rpc_backend
    
    # Set a short timeout
    client.set_timeout(100)  # 100ms
    
    # Call method that sleeps longer than the timeout
    with pytest.raises(Exception) as exc_info:
        await client.call_method("sleep", 0.5)  # 500ms
    
    # Verify timeout error
    assert "timeout" in str(exc_info.value).lower()
    
    # Reset timeout to avoid affecting other tests
    client.set_timeout(30000)  # 30s


@pytest.mark.asyncio
async def test_rpc_fire_method(rpc_backend):
    """Test fire-and-forget method calls."""
    server, client = rpc_backend
    
    # Set up a custom handler to track fired methods
    fired_methods = []
    
    async def track_fired(conn_id, *args):
        fired_methods.append((conn_id, args))
        return "tracked"
    
    server.register_method_handler("track_fired", track_fired)
    
    # Fire method
    await client.fire_method("track_fired", "test_arg", 123)
    
    # Give some time for processing
    await asyncio.sleep(0.1)
    
    # Verify the method was called
    assert len(fired_methods) == 1
    assert fired_methods[0][1] == ("test_arg", 123)


@pytest.mark.asyncio
async def test_rpc_events(rpc_backend):
    """Test RPC events across different backends."""
    server, client = rpc_backend
    
    # Set up event handlers
    received_events = []
    
    async def event_handler(topic, message):
        received_events.append((topic, message))
    
    # Replace the client's event handler
    client_id = client.get_id()
    original_on_event = client.on_event
    client.on_event = event_handler
    
    # Emit event from server to client
    await server.emit(client_id, "test_topic", "test_message")
    
    # Give some time for processing
    await asyncio.sleep(0.1)
    
    # Verify event was received
    assert ("test_topic", "test_message") in received_events
    
    # Restore original handler
    client.on_event = original_on_event


@pytest.mark.asyncio
async def test_rpc_broadcast(rpc_backend):
    """Test RPC broadcast functionality."""
    server, client = rpc_backend
    
    # Set up event handlers
    received_broadcasts = []
    
    async def event_handler(topic, message):
        received_broadcasts.append((topic, message))
    
    # Replace the client's event handler
    client_id = client.get_id()
    original_on_event = client.on_event
    client.on_event = event_handler
    
    # Broadcast event from server
    await server.broadcast("broadcast_topic", "broadcast_message")
    
    # Give some time for processing
    await asyncio.sleep(0.1)
    
    # Verify broadcast was received
    assert ("broadcast_topic", "broadcast_message") in received_broadcasts
    
    # Test blocked topic
    received_broadcasts.clear()
    await server.broadcast("blocked_topic", "should_not_receive")
    
    # Give some time for processing
    await asyncio.sleep(0.1)
    
    # Verify blocked broadcast was not received
    assert len(received_broadcasts) == 0
    
    # Restore original handler
    client.on_event = original_on_event


@pytest.mark.asyncio
async def test_rpc_client_event_emission(rpc_backend):
    """Test client emitting events to the server."""
    server, client = rpc_backend
    
    # Set up server-side tracking of received events
    received_events = []
    
    async def custom_validator(conn_id, message):
        received_events.append((conn_id, "custom_topic", message))
        return True  # Allow broadcasting
    
    server.register_event_validator("custom_topic", custom_validator)
    
    # Emit event from client
    await client.emit("custom_topic", "client_message")
    
    # Give some time for processing
    await asyncio.sleep(0.1)
    
    # Verify event was received and processed by validator
    assert (client.get_id(), "custom_topic", "client_message") in received_events


@pytest.mark.asyncio
async def test_rpc_concurrent_calls(rpc_backend):
    """Test concurrent RPC calls."""
    server, client = rpc_backend
    
    # Make multiple concurrent calls
    tasks = [
        client.call_method("add", i, i)
        for i in range(10)
    ]
    
    # Wait for all calls to complete
    results = await asyncio.gather(*tasks)
    
    # Verify results
    assert results == [i + i for i in range(10)]


@pytest.mark.asyncio
async def test_rpc_disconnect_handling(rpc_backend):
    """Test handling of disconnections."""
    server, client = rpc_backend
    
    # Get client ID
    client_id = client.get_id()
    
    # Verify connection is active
    assert server.is_active(client_id)
    
    # Disconnect client
    await server.disconnect(client_id, "test_disconnect")
    
    # Give some time for processing
    await asyncio.sleep(0.1)
    
    # Verify connection is no longer active
    assert not server.is_active(client_id)
    
    # Verify calling methods on disconnected client fails
    with pytest.raises(Exception):
        await server.call_method(client_id, "echo", "should fail")


@pytest.mark.asyncio
async def test_rpc_wait_next_event(rpc_backend):
    """Test waiting for the next event."""
    server, client = rpc_backend
    
    # Set up a task to emit an event after a delay
    async def delayed_emit():
        await asyncio.sleep(0.1)
        await server.emit(client.get_id(), "delayed_topic", "delayed_message")
    
    # Start the delayed emit task
    task = asyncio.create_task(delayed_emit())
    
    # Wait for the event
    result = await client.wait_next_event("delayed_topic", 1000)
    
    # Verify the result
    assert result == "delayed_message"
    
    # Clean up
    await task


@pytest.mark.asyncio
async def test_rpc_complex_data_structures(rpc_backend):
    """Test handling of complex data structures."""
    server, client = rpc_backend
    
    # Define a complex nested data structure
    complex_data = {
        "string": "value",
        "number": 42,
        "float": 3.14159,
        "boolean": True,
        "null": None,
        "array": [1, 2, 3, "four", 5.0, {"nested": True}],
        "object": {
            "a": 1,
            "b": [2, 3],
            "c": {
                "d": 4,
                "e": [5, 6, 7]
            }
        }
    }
    
    # Echo the complex data
    result = await client.call_method("echo", complex_data)
    
    # Verify the result
    assert result == complex_data


if __name__ == "__main__":
    pytest.main(["-v", __file__])
