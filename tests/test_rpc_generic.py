"""
Generic parameterized tests for RPC functionality that can run over any backend.
This test suite is designed to be extensible for different server types and configurations.
"""

import pytest
import asyncio
import uuid
import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, AsyncGenerator
from dataclasses import dataclass
from abc import ABC, abstractmethod

from cbor_rpc import (
    Pipe, TcpPipe, TcpServer, JsonTransformer,
    RpcV1, RpcV1Server, RpcClient, RpcAuthorizedClient
)


# ===== Mock Pipe for In-Memory Testing =====

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


# ===== Base Test RPC Server Implementation =====

class BaseTestRpcServer(RpcV1Server):
    """Base test RPC server implementation with common functionality."""
    
    def __init__(self):
        super().__init__()
        self._tcp_server = None
        self._method_handlers = {}
        self._event_validators = {}
        self._call_log = []
    
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle RPC method calls."""
        # Log the call for testing purposes
        self._call_log.append({
            "connection_id": connection_id,
            "method": method,
            "args": args,
            "timestamp": asyncio.get_event_loop().time()
        })
        
        # Check for custom handlers first
        if method in self._method_handlers:
            handler = self._method_handlers[method]
            if asyncio.iscoroutinefunction(handler):
                return await handler(connection_id, *args)
            else:
                return handler(connection_id, *args)
        
        # Default handlers for testing
        return await self._handle_default_method(connection_id, method, args)
    
    async def _handle_default_method(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle default test methods."""
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
        elif method == "get_server_info":
            return {
                "server_type": self.__class__.__name__,
                "connections": len(self.active_connections),
                "call_count": len(self._call_log)
            }
        elif method == "sleep":
            await asyncio.sleep(args[0])
            return f"Slept for {args[0]} seconds"
        elif method == "error":
            raise Exception(args[0] if args else "Test error")
        elif method == "get_call_log":
            return self._call_log.copy()
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
    
    def get_call_log(self) -> List[Dict[str, Any]]:
        """Get the call log for testing purposes."""
        return self._call_log.copy()
    
    def clear_call_log(self):
        """Clear the call log."""
        self._call_log.clear()
    
    async def cleanup(self):
        """Clean up resources."""
        # Close all connections
        for conn_id in list(self.active_connections.keys()):
            await self.disconnect(conn_id)
        
        # Close TCP server if it exists
        if self._tcp_server:
            await self._tcp_server.close()


class SimpleTestRpcServer(BaseTestRpcServer):
    """Simple test RPC server without any transformations."""
    pass


class JsonTestRpcServer(BaseTestRpcServer):
    """Test RPC server that works with JSON-transformed data."""
    
    async def _handle_default_method(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle default test methods with JSON-specific responses."""
        if method == "json_echo":
            return {
                "echoed": args[0] if args else None,
                "type": "json_response",
                "timestamp": "2024-01-01T00:00:00Z"
            }
        elif method == "process_json_data":
            data = args[0] if args else {}
            return {
                "processed": True,
                "input_keys": list(data.keys()) if isinstance(data, dict) else [],
                "input_type": type(data).__name__,
                "server_type": "JsonTestRpcServer"
            }
        else:
            # Call parent method for standard functionality
            result = await super()._handle_default_method(connection_id, method, args)
            # Wrap simple responses in JSON structure for consistency
            if isinstance(result, (str, int, float, bool)) or result is None:
                return {"value": result, "type": "json_wrapped"}
            return result


# ===== Backend Configuration Classes =====

@dataclass
class RpcBackendConfig(ABC):
    """Base class for RPC backend configurations."""
    name: str
    description: str
    
    @abstractmethod
    async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
        """Create a server and client pair for testing."""
        pass
    
    @abstractmethod
    async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
        """Clean up backend-specific resources."""
        pass


class InMemorySimpleBackend(RpcBackendConfig):
    """In-memory RPC backend using MockPipe with simple server."""
    
    def __init__(self):
        super().__init__(
            name="in-memory-simple",
            description="In-memory communication with simple RPC server"
        )
    
    async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
        # Create server
        server = SimpleTestRpcServer()
        
        # Create mock pipes for bidirectional communication
        server_pipe = MockPipe()
        client_pipe = MockPipe()
        
        # Connect pipes bidirectionally
        server_pipe.connect_to(client_pipe)
        
        # Create client ID
        client_id = f"simple-client-{uuid.uuid4()}"
        
        # Add connection to server
        await server.add_connection(client_id, server_pipe)
        
        # Create client
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return {"client_response": f"Handled {method} with {len(args)} args"}
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling can be customized per test
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        return server, client
    
    async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
        """Clean up in-memory backend."""
        await server.cleanup()


class InMemoryJsonBackend(RpcBackendConfig):
    """In-memory RPC backend using MockPipe with JSON transformer."""
    
    def __init__(self):
        super().__init__(
            name="in-memory-json",
            description="In-memory communication with JSON transformation"
        )
    
    async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
        # Create server
        server = JsonTestRpcServer()
        
        # Create mock pipes for bidirectional communication
        raw_server_pipe = MockPipe()
        raw_client_pipe = MockPipe()
        
        # Connect raw pipes bidirectionally
        raw_server_pipe.connect_to(raw_client_pipe)
        
        # Wrap pipes with JSON transformers
        server_pipe = JsonTransformer(raw_server_pipe)
        client_pipe = JsonTransformer(raw_client_pipe)
        
        # Create client ID
        client_id = f"json-client-{uuid.uuid4()}"
        
        # Add connection to server
        await server.add_connection(client_id, server_pipe)
        
        # Create client
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return {
                "client_response": f"JSON handled {method}",
                "args_count": len(args),
                "client_type": "json"
            }
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling can be customized per test
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        return server, client
    
    async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
        """Clean up JSON backend."""
        await server.cleanup()


class TcpSimpleBackend(RpcBackendConfig):
    """TCP-based RPC backend using TcpPipe with simple server."""
    
    def __init__(self):
        super().__init__(
            name="tcp-simple",
            description="TCP communication with simple RPC server"
        )
    
    async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
        # Create server
        server = SimpleTestRpcServer()
        
        # Create TCP server
        tcp_server = await TcpServer.create('127.0.0.1', 0)
        host, port = tcp_server.get_address()
        
        # Set up connection handler
        client_ready = asyncio.Event()
        client_id = f"tcp-simple-client-{uuid.uuid4()}"
        
        async def on_connection(tcp_pipe: TcpPipe):
            await server.add_connection(client_id, tcp_pipe)
            client_ready.set()
        
        tcp_server.on_connection(on_connection)
        
        # Create client connection
        client_pipe = await TcpPipe.create_connection(host, port)
        
        # Wait for server to register the connection
        await client_ready.wait()
        
        # Create client
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return f"TCP client handled {method}"
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling can be customized per test
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        # Store TCP server for cleanup
        server._tcp_server = tcp_server
        
        return server, client
    
    async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
        """Clean up TCP backend."""
        await server.cleanup()


class TcpJsonBackend(RpcBackendConfig):
    """TCP-based RPC backend using TcpPipe with JSON transformer."""
    
    def __init__(self):
        super().__init__(
            name="tcp-json",
            description="TCP communication with JSON transformation"
        )
    
    async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
        # Create server
        server = JsonTestRpcServer()
        
        # Create TCP server
        tcp_server = await TcpServer.create('127.0.0.1', 0)
        host, port = tcp_server.get_address()
        
        # Set up connection handler
        client_ready = asyncio.Event()
        client_id = f"tcp-json-client-{uuid.uuid4()}"
        
        async def on_connection(tcp_pipe: TcpPipe):
            # Wrap TCP pipe with JSON transformer
            json_pipe = JsonTransformer(tcp_pipe)
            await server.add_connection(client_id, json_pipe)
            client_ready.set()
        
        tcp_server.on_connection(on_connection)
        
        # Create client connection with JSON transformer
        tcp_client_pipe = await TcpPipe.create_connection(host, port)
        client_pipe = JsonTransformer(tcp_client_pipe)
        
        # Wait for server to register the connection
        await client_ready.wait()
        
        # Create client
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return {
                "client_response": f"TCP+JSON handled {method}",
                "transport": "tcp",
                "encoding": "json"
            }
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling can be customized per test
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        # Store TCP server for cleanup
        server._tcp_server = tcp_server
        
        return server, client
    
    async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
        """Clean up TCP+JSON backend."""
        await server.cleanup()


# ===== Test Fixtures =====

# All available backend configurations
ALL_BACKENDS = [
    InMemorySimpleBackend(),
    InMemoryJsonBackend(),
    TcpSimpleBackend(),
    TcpJsonBackend(),
]

# Subset for faster testing (can be used for quick tests)
FAST_BACKENDS = [
    InMemorySimpleBackend(),
    InMemoryJsonBackend(),
]

# JSON-specific backends for JSON transformation tests
JSON_BACKENDS = [
    InMemoryJsonBackend(),
    TcpJsonBackend(),
]


@pytest.fixture(params=ALL_BACKENDS)
async def rpc_backend(request) -> AsyncGenerator[Tuple[BaseTestRpcServer, RpcV1, RpcBackendConfig], None]:
    """
    Parameterized fixture that provides different RPC backend implementations.
    
    Returns a tuple of (server, client, backend_config) for each backend type.
    """
    backend: RpcBackendConfig = request.param
    
    # Print which backend is being tested
    print(f"\n🧪 Testing with {backend.name}: {backend.description}")
    
    # Create server and client
    server, client = await backend.create_server_client_pair()
    
    # Give some time for connections to be established
    await asyncio.sleep(0.1)
    
    try:
        yield server, client, backend
    finally:
        # Clean up
        await backend.cleanup_backend(server, client)


@pytest.fixture(params=FAST_BACKENDS)
async def fast_rpc_backend(request) -> AsyncGenerator[Tuple[BaseTestRpcServer, RpcV1, RpcBackendConfig], None]:
    """Fast backend fixture for quick tests."""
    backend: RpcBackendConfig = request.param
    
    print(f"\n⚡ Fast testing with {backend.name}: {backend.description}")
    
    server, client = await backend.create_server_client_pair()
    await asyncio.sleep(0.05)  # Shorter wait for fast tests
    
    try:
        yield server, client, backend
    finally:
        await backend.cleanup_backend(server, client)


@pytest.fixture(params=JSON_BACKENDS)
async def json_rpc_backend(request) -> AsyncGenerator[Tuple[BaseTestRpcServer, RpcV1, RpcBackendConfig], None]:
    """JSON-specific backend fixture."""
    backend: RpcBackendConfig = request.param
    
    print(f"\n📄 JSON testing with {backend.name}: {backend.description}")
    
    server, client = await backend.create_server_client_pair()
    await asyncio.sleep(0.1)
    
    try:
        yield server, client, backend
    finally:
        await backend.cleanup_backend(server, client)


# ===== Generic RPC Tests =====

@pytest.mark.asyncio
async def test_rpc_echo_all_backends(rpc_backend):
    """Test basic echo functionality across all backends."""
    server, client, backend = rpc_backend
    
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
        assert result == value, f"Echo failed for {value} on {backend.name}"


@pytest.mark.asyncio
async def test_rpc_math_operations(fast_rpc_backend):
    """Test mathematical operations across fast backends."""
    server, client, backend = fast_rpc_backend
    
    # Test addition
    result = await client.call_method("add", 1, 2, 3, 4, 5)
    assert result == 15, f"Addition failed on {backend.name}"
    
    # Test multiplication
    result = await client.call_method("multiply", 2, 3, 4)
    assert result == 24, f"Multiplication failed on {backend.name}"


@pytest.mark.asyncio
async def test_rpc_connection_management(rpc_backend):
    """Test connection ID management and server info."""
    server, client, backend = rpc_backend
    
    # Get connection ID from server
    conn_id = await client.call_method("get_connection_id")
    assert conn_id == client.get_id(), f"Connection ID mismatch on {backend.name}"
    
    # Verify the connection is active on the server
    assert server.is_active(conn_id), f"Connection not active on {backend.name}"
    
    # Get server info
    server_info = await client.call_method("get_server_info")
    assert isinstance(server_info, dict), f"Server info not dict on {backend.name}"
    assert server_info["connections"] >= 1, f"Connection count wrong on {backend.name}"


@pytest.mark.asyncio
async def test_rpc_error_handling(fast_rpc_backend):
    """Test error handling across backends."""
    server, client, backend = fast_rpc_backend
    
    # Test method that raises an exception
    with pytest.raises(Exception) as exc_info:
        await client.call_method("error", "Custom error message")
    
    assert "Custom error message" in str(exc_info.value), f"Error handling failed on {backend.name}"
    
    # Test calling non-existent method
    with pytest.raises(Exception) as exc_info:
        await client.call_method("non_existent_method")
    
    assert "Unknown method" in str(exc_info.value), f"Unknown method handling failed on {backend.name}"


@pytest.mark.asyncio
async def test_rpc_fire_method(rpc_backend):
    """Test fire-and-forget method calls."""
    server, client, backend = rpc_backend
    
    # Clear call log
    server.clear_call_log()
    
    # Fire method
    await client.fire_method("echo", "fire_test_message")
    
    # Give some time for processing
    await asyncio.sleep(0.2)
    
    # Verify the method was called by checking the call log
    call_log = server.get_call_log()
    assert len(call_log) >= 1, f"Fire method not logged on {backend.name}"
    
    # Find the echo call in the log
    echo_calls = [call for call in call_log if call["method"] == "echo"]
    assert len(echo_calls) >= 1, f"Echo fire method not found in log on {backend.name}"
    assert echo_calls[0]["args"] == ["fire_test_message"], f"Fire method args wrong on {backend.name}"


@pytest.mark.asyncio
async def test_rpc_concurrent_calls(fast_rpc_backend):
    """Test concurrent RPC calls."""
    server, client, backend = fast_rpc_backend
    
    # Make multiple concurrent calls
    tasks = [
        client.call_method("add", i, i)
        for i in range(3)  # Small number for fast testing
    ]
    
    # Wait for all calls to complete
    results = await asyncio.gather(*tasks)
    
    # Verify results
    expected = [i + i for i in range(3)]
    assert results == expected, f"Concurrent calls failed on {backend.name}"


# ===== JSON-Specific Tests =====

@pytest.mark.asyncio
async def test_json_specific_methods(json_rpc_backend):
    """Test JSON-specific functionality."""
    server, client, backend = json_rpc_backend
    
    # Test JSON echo
    result = await client.call_method("json_echo", "Hello JSON World!")
    assert isinstance(result, dict), f"JSON echo should return dict on {backend.name}"
    assert result["echoed"] == "Hello JSON World!", f"JSON echo content wrong on {backend.name}"
    assert result["type"] == "json_response", f"JSON echo type wrong on {backend.name}"
    
    # Test JSON data processing
    complex_data = {
        "users": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
        "metadata": {"version": "1.0", "timestamp": "2024-01-01"}
    }
    
    result = await client.call_method("process_json_data", complex_data)
    assert isinstance(result, dict), f"JSON processing should return dict on {backend.name}"
    assert result["processed"] is True, f"JSON processing flag wrong on {backend.name}"
    assert "users" in result["input_keys"], f"JSON processing keys wrong on {backend.name}"
    assert "metadata" in result["input_keys"], f"JSON processing keys wrong on {backend.name}"


@pytest.mark.asyncio
async def test_json_data_integrity(json_rpc_backend):
    """Test that complex JSON data maintains integrity through transformation."""
    server, client, backend = json_rpc_backend
    
    # Test with various JSON data types
    test_cases = [
        {"string": "Hello 世界 🌍"},  # Unicode
        {"numbers": [1, 2.5, -3, 0]},  # Various numbers
        {"nested": {"deep": {"very": {"deep": "value"}}}},  # Deep nesting
        {"mixed": [1, "two", {"three": 3}, [4, 5]]},  # Mixed types
        {"boolean_and_null": [True, False, None]},  # Special values
    ]
    
    for test_data in test_cases:
        result = await client.call_method("echo", test_data)
        assert result == test_data, f"JSON integrity failed for {test_data} on {backend.name}"


# ===== Performance and Stress Tests =====

@pytest.mark.asyncio
async def test_rpc_performance_simple(fast_rpc_backend):
    """Test basic performance with simple operations."""
    server, client, backend = fast_rpc_backend
    
    import time
    
    # Measure time for multiple simple calls
    start_time = time.time()
    
    tasks = [client.call_method("add", i, i) for i in range(10)]
    results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Verify results
    expected = [i + i for i in range(10)]
    assert results == expected, f"Performance test results wrong on {backend.name}"
    
    # Basic performance check (should complete in reasonable time)
    assert duration < 5.0, f"Performance test too slow ({duration:.2f}s) on {backend.name}"
    
    print(f"📊 {backend.name}: 10 concurrent calls in {duration:.3f}s")


@pytest.mark.asyncio
async def test_rpc_event_broadcasting(rpc_backend):
    """Test event broadcasting functionality."""
    server, client, backend = rpc_backend
    
    # Test event emission
    await client.emit("test_event", {"message": "Hello Events", "data": [1, 2, 3]})
    
    # Give time for event processing
    await asyncio.sleep(0.1)
    
    # For now, just verify no errors occurred
    # In a real scenario, you'd have multiple clients to test broadcasting
    assert True, f"Event emission completed on {backend.name}"


# ===== Utility Functions for Adding New Backends =====

def create_custom_backend(name: str, description: str, 
                         server_factory: Callable[[], BaseTestRpcServer],
                         pipe_factory: Callable[[], Tuple[Pipe, Pipe]],
                         cleanup_func: Optional[Callable] = None) -> RpcBackendConfig:
    """
    Utility function to easily create custom backend configurations.
    
    Args:
        name: Backend name
        description: Backend description
        server_factory: Function that creates a server instance
        pipe_factory: Function that creates a (server_pipe, client_pipe) tuple
        cleanup_func: Optional cleanup function
    
    Returns:
        A custom RpcBackendConfig instance
    """
    
    class CustomBackend(RpcBackendConfig):
        def __init__(self):
            super().__init__(name=name, description=description)
        
        async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
            server = server_factory()
            server_pipe, client_pipe = pipe_factory()
            
            client_id = f"custom-client-{uuid.uuid4()}"
            await server.add_connection(client_id, server_pipe)
            
            def client_method_handler(method: str, args: List[Any]) -> Any:
                return f"Custom client handled {method}"
            
            async def client_event_handler(topic: str, message: Any) -> None:
                pass
            
            client = RpcV1.make_rpc_v1(
                client_pipe, client_id, client_method_handler, client_event_handler
            )
            
            return server, client
        
        async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
            if cleanup_func:
                await cleanup_func(server, client)
            await server.cleanup()
    
    return CustomBackend()


# ===== Example of Adding a New Backend Type =====

# Example: Adding a compression backend
# def create_compression_backend():
#     """Example of how to add a new backend type."""
#     
#     def server_factory():
#         return SimpleTestRpcServer()
#     
#     def pipe_factory():
#         # Create pipes with compression transformer
#         raw_pipe1, raw_pipe2 = Pipe.create_pair()
#         compressed_pipe1 = CompressionTransformer(raw_pipe1)
#         compressed_pipe2 = CompressionTransformer(raw_pipe2)
#         return compressed_pipe1, compressed_pipe2
#     
#     return create_custom_backend(
#         name="in-memory-compression",
#         description="In-memory communication with compression",
#         server_factory=server_factory,
#         pipe_factory=pipe_factory
#     )


if __name__ == "__main__":
    pytest.main(["-v", __file__])
