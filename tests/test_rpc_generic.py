"""
Generic parameterized tests for RPC functionality that can run over any backend.
"""

import pytest
import asyncio
import uuid
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, Type, AsyncGenerator
from dataclasses import dataclass

from cbor_rpc import (
    Pipe,  TcpPipe, TcpServer,
    RpcV1, RpcV1Server, RpcClient, RpcAuthorizedClient
)


from cbor_rpc.pipe import T1



class MockPipe(Pipe[Any, Any]):
    """A mock pipe that can be connected to another mock pipe for testing."""
    
    def __init__(self):
        super().__init__()
        self.written_data = []
        self.connected_pipe: Optional['MockPipe'] = None
        self._closed = False
    
    def connect_to(self, other_pipe: 'MockPipe'):
        """Connect this pipe to another pipe for bidirectional communication."""
        self.connected_pipe = other_pipe
        other_pipe.connected_pipe = self
    
    async def write(self, chunk: Any) -> bool:
        """Write data to this pipe and forward to connected pipe."""
        if self._closed:
            return False
            
        self.written_data.append(chunk)
        
        # Forward to connected pipe
        if self.connected_pipe and not self.connected_pipe._closed:
            await self.connected_pipe._emit("data", chunk)
        
        return True
    
    async def terminate(self, *args: Any) -> None:
        """Terminate the pipe."""
        if self._closed:
            return
            
        self._closed = True
        await self._emit("close", *args)
        
        # Notify connected pipe
        if self.connected_pipe and not self.connected_pipe._closed:
            await self.connected_pipe._emit("close", *args)


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
    """In-memory RPC backend using MockPipe."""
    
    def __init__(self):
        super().__init__(name="in-memory")
    
    @staticmethod
    async def create_server_client_pair() -> Tuple[RpcV1Server, RpcV1]:
        # Create server
        server = TestRpcServer()
        
        # Create mock pipes for bidirectional communication
        server_pipe = MockPipe()
        client_pipe = MockPipe()
        
        # Connect pipes bidirectionally
        server_pipe.connect_to(client_pipe)
        
        # Create client ID
        client_id = str(uuid.uuid4())
        
        # Add connection to server
        await server.add_connection(client_id, server_pipe)
        
        # Create client
        def client_method_handler(method: str, args: List[Any]) -> Any:
            # Client doesn't handle methods in this test
            raise Exception("Client doesn't handle method calls")
        
        async def client_event_handler(topic: str, message: Any) -> None:
            # Client event handling can be customized per test
            pass
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        return server, client


class TcpRpcBackend(RpcBackendConfig):
    """TCP-based RPC backend using TcpPipe."""
    
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
        
        async def on_connection(tcp_duplex: TcpPipe):
            await server.add_connection(client_id, tcp_duplex)
            client_ready.set()
        
        tcp_server.on_connection(on_connection)
        
        # Create client connection
        client_pipe = await TcpPipe.create_connection(host, port)
        
        # Wait for server to register the connection
        await client_ready.wait()
        
        # Create client
        def client_method_handler(method: str, args: List[Any]) -> Any:
            # Client doesn't handle methods in this test
            raise Exception("Client doesn't handle method calls")
        
        async def client_event_handler(topic: str, message: Any) -> None:
            # Client event handling can be customized per test
            pass
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
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
            handler = self._method_handlers[method]
            if asyncio.iscoroutinefunction(handler):
                return await handler(connection_id, *args)
            else:
                return handler(connection_id, *args)
        
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
            validator = self._event_validators[topic]
            if asyncio.iscoroutinefunction(validator):
                return await validator(connection_id, message)
            else:
                return validator(connection_id, message)
        
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
    
    # Give some time for connections to be established
    await asyncio.sleep(0.1)
    
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
        {"key": "value"}
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
async def test_rpc_fire_method(rpc_backend):
    """Test fire-and-forget method calls."""
    server, client = rpc_backend
    
    # Set up a custom handler to track fired methods
    fired_methods = []
    
    def track_fired(conn_id, *args):
        fired_methods.append((conn_id, args))
        return "tracked"
    
    server.register_method_handler("track_fired", track_fired)
    
    # Fire method
    await client.fire_method("track_fired", "test_arg", 123)
    
    # Give some time for processing
    await asyncio.sleep(0.2)
    
    # Verify the method was called
    assert len(fired_methods) == 1
    assert fired_methods[0][1] == ("test_arg", 123)


@pytest.mark.asyncio
async def test_rpc_concurrent_calls(rpc_backend):
    """Test concurrent RPC calls."""
    server, client = rpc_backend
    
    # Make multiple concurrent calls
    tasks = [
        client.call_method("add", i, i)
        for i in range(3)  # Reduced for faster testing
    ]
    
    # Wait for all calls to complete
    results = await asyncio.gather(*tasks)
    
    # Verify results
    assert results == [i + i for i in range(3)]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
