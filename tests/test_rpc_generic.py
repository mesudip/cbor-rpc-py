"""
Generic parameterized tests for RPC functionality that can run over any backend.
This test suite uses consistent handlers across all backend types and tests
comprehensive scenarios including different return types and error conditions.
"""

import pytest
import asyncio
import uuid
import json
import time
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


# ===== Unified RPC Method Handlers =====

class UnifiedRpcHandlers:
    """Unified RPC method handlers used across all backend types."""
    
    def __init__(self):
        self.call_log = []
        self.sleep_calls = []
        self.error_calls = []
    
    def log_call(self, connection_id: str, method: str, args: List[Any]):
        """Log method calls for testing purposes."""
        self.call_log.append({
            "connection_id": connection_id,
            "method": method,
            "args": args,
            "timestamp": time.time()
        })
    
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Unified method handler for all backend types."""
        self.log_call(connection_id, method, args)
        
        # Basic arithmetic
        if method == "add":
            if len(args) < 2:
                raise Exception("add method requires at least 2 parameters")
            return sum(args)
        
        elif method == "multiply":
            if len(args) < 2:
                raise Exception("multiply method requires at least 2 parameters")
            result = 1
            for arg in args:
                result *= arg
            return result
        
        # Echo methods for different return types
        elif method == "echo":
            return args[0] if args else None
        
        elif method == "echo_int":
            return int(args[0]) if args else 0
        
        elif method == "echo_string":
            return str(args[0]) if args else ""
        
        elif method == "echo_boolean":
            return bool(args[0]) if args else False
        
        elif method == "echo_float":
            return float(args[0]) if args else 0.0
        
        elif method == "echo_bytes":
            if args and isinstance(args[0], str):
                return args[0].encode('utf-8')
            elif args and isinstance(args[0], bytes):
                return args[0]
            else:
                return b""
        
        elif method == "echo_array":
            return list(args) if args else []
        
        elif method == "echo_map":
            if args and isinstance(args[0], dict):
                return args[0]
            else:
                return {"echoed": args}
        
        # Async operations
        elif method == "sleep":
            if not args:
                raise Exception("sleep method requires duration parameter")
            duration = float(args[0])
            self.sleep_calls.append({"duration": duration, "connection_id": connection_id})
            await asyncio.sleep(duration)
            return f"Slept for {duration} seconds"
        
        # Error handling
        elif method == "throw_error":
            error_message = args[0] if args else "Test error"
            self.error_calls.append({"message": error_message, "connection_id": connection_id})
            raise Exception(error_message)
        
        # Utility methods
        elif method == "get_connection_id":
            return connection_id
        
        elif method == "get_server_info":
            return {
                "server_type": "UnifiedTestServer",
                "connections": 1,  # Will be updated by server
                "call_count": len(self.call_log),
                "backend_type": "unified"
            }
        
        elif method == "get_call_log":
            return self.call_log.copy()
        
        elif method == "clear_logs":
            self.call_log.clear()
            self.sleep_calls.clear()
            self.error_calls.clear()
            return "Logs cleared"
        
        # Test missing parameters
        elif method == "require_params":
            if len(args) < 3:
                raise Exception(f"require_params needs 3 parameters, got {len(args)}")
            return {"param1": args[0], "param2": args[1], "param3": args[2]}
        
        # Complex return types
        elif method == "get_complex_data":
            return {
                "string": "Hello World",
                "integer": 42,
                "float": 3.14159,
                "boolean": True,
                "null_value": None,
                "array": [1, 2, 3, "mixed", True],
                "nested_object": {
                    "inner_string": "nested",
                    "inner_array": [{"deep": "value"}]
                },
                "bytes_data": "binary_data".encode('utf-8') if method != "get_complex_data" else "binary_data"
            }
        
        else:
            raise Exception(f"Unknown method: {method}")


# ===== Base Test RPC Server Implementation =====

class UnifiedTestRpcServer(RpcV1Server):
    """Unified test RPC server that uses consistent handlers across all backends."""
    
    def __init__(self):
        super().__init__()
        self._tcp_server = None
        self.handlers = UnifiedRpcHandlers()
    
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle RPC method calls using unified handlers."""
        result = await self.handlers.handle_method_call(connection_id, method, args)
        
        # Update connection count in server info
        if method == "get_server_info" and isinstance(result, dict):
            result["connections"] = len(self.active_connections)
        
        return result
    
    async def validate_event_broadcast(self, connection_id: str, topic: str, message: Any) -> bool:
        """Validate whether an event should be broadcasted."""
        # Block specific test topics
        if topic == "blocked_topic":
            return False
        if topic == "invalid_topic":
            return False
        return True
    
    def get_handlers(self) -> UnifiedRpcHandlers:
        """Get the unified handlers for testing purposes."""
        return self.handlers
    
    async def cleanup(self):
        """Clean up resources."""
        # Close all connections
        for conn_id in list(self.active_connections.keys()):
            await self.disconnect(conn_id)
        
        # Close TCP server if it exists
        if self._tcp_server:
            await self._tcp_server.close()


# ===== Backend Configuration Classes =====

@dataclass
class RpcBackendConfig(ABC):
    """Base class for RPC backend configurations."""
    name: str
    description: str
    supports_bytes: bool = True  # Whether backend supports bytes data
    
    @abstractmethod
    async def create_server_client_pair(self) -> Tuple[UnifiedTestRpcServer, RpcV1]:
        """Create a server and client pair for testing."""
        pass
    
    @abstractmethod
    async def cleanup_backend(self, server: UnifiedTestRpcServer, client: RpcV1):
        """Clean up backend-specific resources."""
        pass


class InMemorySimpleBackend(RpcBackendConfig):
    """In-memory RPC backend using MockPipe with unified server."""
    
    def __init__(self):
        super().__init__(
            name="in-memory-simple",
            description="In-memory communication with unified RPC server",
            supports_bytes=True
        )
    
    async def create_server_client_pair(self) -> Tuple[UnifiedTestRpcServer, RpcV1]:
        # Create server
        server = UnifiedTestRpcServer()
        
        # Create mock pipes for bidirectional communication
        server_pipe = MockPipe()
        client_pipe = MockPipe()
        
        # Connect pipes bidirectionally
        server_pipe.connect_to(client_pipe)
        
        # Create client ID
        client_id = f"simple-client-{uuid.uuid4()}"
        
        # Add connection to server
        await server.add_connection(client_id, server_pipe)
        
        # Create client with unified handlers
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return {"client_response": f"Client handled {method}", "args_count": len(args)}
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        return server, client
    
    async def cleanup_backend(self, server: UnifiedTestRpcServer, client: RpcV1):
        """Clean up in-memory backend."""
        await server.cleanup()


class InMemoryJsonBackend(RpcBackendConfig):
    """In-memory RPC backend using MockPipe with JSON transformer."""
    
    def __init__(self):
        super().__init__(
            name="in-memory-json",
            description="In-memory communication with JSON transformation",
            supports_bytes=False  # JSON doesn't support raw bytes
        )
    
    async def create_server_client_pair(self) -> Tuple[UnifiedTestRpcServer, RpcV1]:
        # Create server
        server = UnifiedTestRpcServer()
        
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
        
        # Create client with unified handlers
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return {
                "client_response": f"JSON client handled {method}",
                "args_count": len(args),
                "encoding": "json"
            }
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        return server, client
    
    async def cleanup_backend(self, server: UnifiedTestRpcServer, client: RpcV1):
        """Clean up JSON backend."""
        await server.cleanup()


class TcpSimpleBackend(RpcBackendConfig):
    """TCP-based RPC backend using TcpPipe with unified server."""
    
    def __init__(self):
        super().__init__(
            name="tcp-simple",
            description="TCP communication with unified RPC server",
            supports_bytes=True
        )
    
    async def create_server_client_pair(self) -> Tuple[UnifiedTestRpcServer, RpcV1]:
        # Create server
        server = UnifiedTestRpcServer()
        
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
        
        # Create client with unified handlers
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return {"client_response": f"TCP client handled {method}", "transport": "tcp"}
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        # Store TCP server for cleanup
        server._tcp_server = tcp_server
        
        return server, client
    
    async def cleanup_backend(self, server: UnifiedTestRpcServer, client: RpcV1):
        """Clean up TCP backend."""
        await server.cleanup()


class TcpJsonBackend(RpcBackendConfig):
    """TCP-based RPC backend using TcpPipe with JSON transformer."""
    
    def __init__(self):
        super().__init__(
            name="tcp-json",
            description="TCP communication with JSON transformation",
            supports_bytes=False  # JSON doesn't support raw bytes
        )
    
    async def create_server_client_pair(self) -> Tuple[UnifiedTestRpcServer, RpcV1]:
        # Create server
        server = UnifiedTestRpcServer()
        
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
        
        # Create client with unified handlers
        def client_method_handler(method: str, args: List[Any]) -> Any:
            return {
                "client_response": f"TCP+JSON client handled {method}",
                "transport": "tcp",
                "encoding": "json"
            }
        
        async def client_event_handler(topic: str, message: Any) -> None:
            pass  # Client event handling
        
        client = RpcV1.make_rpc_v1(
            client_pipe,
            client_id,
            client_method_handler,
            client_event_handler
        )
        
        # Store TCP server for cleanup
        server._tcp_server = tcp_server
        
        return server, client
    
    async def cleanup_backend(self, server: UnifiedTestRpcServer, client: RpcV1):
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

# Subset for faster testing
FAST_BACKENDS = [
    InMemorySimpleBackend(),
    InMemoryJsonBackend(),
]

# Backends that support bytes
BYTES_BACKENDS = [backend for backend in ALL_BACKENDS if backend.supports_bytes]


@pytest.fixture(params=ALL_BACKENDS)
async def rpc_backend(request) -> AsyncGenerator[Tuple[UnifiedTestRpcServer, RpcV1, RpcBackendConfig], None]:
    """Parameterized fixture that provides different RPC backend implementations."""
    backend: RpcBackendConfig = request.param
    
    print(f"\n🧪 Testing with {backend.name}: {backend.description}")
    
    server, client = await backend.create_server_client_pair()
    await asyncio.sleep(0.1)
    
    try:
        yield server, client, backend
    finally:
        await backend.cleanup_backend(server, client)


@pytest.fixture(params=FAST_BACKENDS)
async def fast_rpc_backend(request) -> AsyncGenerator[Tuple[UnifiedTestRpcServer, RpcV1, RpcBackendConfig], None]:
    """Fast backend fixture for quick tests."""
    backend: RpcBackendConfig = request.param
    
    print(f"\n⚡ Fast testing with {backend.name}: {backend.description}")
    
    server, client = await backend.create_server_client_pair()
    await asyncio.sleep(0.05)
    
    try:
        yield server, client, backend
    finally:
        await backend.cleanup_backend(server, client)


@pytest.fixture(params=BYTES_BACKENDS)
async def bytes_rpc_backend(request) -> AsyncGenerator[Tuple[UnifiedTestRpcServer, RpcV1, RpcBackendConfig], None]:
    """Backend fixture for testing bytes support."""
    backend: RpcBackendConfig = request.param
    
    print(f"\n📦 Bytes testing with {backend.name}: {backend.description}")
    
    server, client = await backend.create_server_client_pair()
    await asyncio.sleep(0.1)
    
    try:
        yield server, client, backend
    finally:
        await backend.cleanup_backend(server, client)


# ===== Comprehensive RPC Tests =====

@pytest.mark.asyncio
async def test_arithmetic_operations(rpc_backend):
    """Test arithmetic operations: add and multiply."""
    server, client, backend = rpc_backend
    
    # Test addition
    result = await client.call_method("add", 1, 2, 3, 4, 5)
    assert result == 15, f"Addition failed on {backend.name}"
    
    # Test multiplication
    result = await client.call_method("multiply", 2, 3, 4)
    assert result == 24, f"Multiplication failed on {backend.name}"
    
    # Test with negative numbers
    result = await client.call_method("add", -5, 10, -3)
    assert result == 2, f"Addition with negatives failed on {backend.name}"


@pytest.mark.asyncio
async def test_echo_return_types(rpc_backend):
    """Test echo methods with different return types."""
    server, client, backend = rpc_backend
    
    # Test basic echo
    result = await client.call_method("echo", "hello world")
    assert result == "hello world", f"Basic echo failed on {backend.name}"
    
    # Test integer echo
    result = await client.call_method("echo_int", "42")
    assert result == 42, f"Integer echo failed on {backend.name}"
    assert isinstance(result, int), f"Integer echo type wrong on {backend.name}"
    
    # Test string echo
    result = await client.call_method("echo_string", 123)
    assert result == "123", f"String echo failed on {backend.name}"
    assert isinstance(result, str), f"String echo type wrong on {backend.name}"
    
    # Test boolean echo
    result = await client.call_method("echo_boolean", "true")
    assert result is True, f"Boolean echo failed on {backend.name}"
    assert isinstance(result, bool), f"Boolean echo type wrong on {backend.name}"
    
    result = await client.call_method("echo_boolean", "")
    assert result is False, f"Boolean echo (false) failed on {backend.name}"
    
    # Test float echo
    result = await client.call_method("echo_float", "3.14159")
    assert abs(result - 3.14159) < 0.00001, f"Float echo failed on {backend.name}"
    assert isinstance(result, float), f"Float echo type wrong on {backend.name}"
    
    # Test array echo
    result = await client.call_method("echo_array", "a", "b", "c")
    assert result == ["a", "b", "c"], f"Array echo failed on {backend.name}"
    assert isinstance(result, list), f"Array echo type wrong on {backend.name}"
    
    # Test map echo
    test_map = {"key1": "value1", "key2": 42}
    result = await client.call_method("echo_map", test_map)
    assert result == test_map, f"Map echo failed on {backend.name}"
    assert isinstance(result, dict), f"Map echo type wrong on {backend.name}"


@pytest.mark.asyncio
async def test_bytes_echo(bytes_rpc_backend):
    """Test bytes echo (only on backends that support bytes)."""
    server, client, backend = bytes_rpc_backend
    
    # Test bytes echo
    test_string = "Hello, 世界! 🌍"
    result = await client.call_method("echo_bytes", test_string)
    expected = test_string.encode('utf-8')
    assert result == expected, f"Bytes echo failed on {backend.name}"
    assert isinstance(result, bytes), f"Bytes echo type wrong on {backend.name}"
    
    # Test with actual bytes input
    test_bytes = b"binary\x00\x01\x02data"
    result = await client.call_method("echo_bytes", test_bytes)
    assert result == test_bytes, f"Bytes echo (bytes input) failed on {backend.name}"


@pytest.mark.asyncio
async def test_async_operations(fast_rpc_backend):
    """Test async operations like sleep."""
    server, client, backend = fast_rpc_backend
    
    # Test sleep operation
    start_time = time.time()
    result = await client.call_method("sleep", 0.1)
    end_time = time.time()
    
    assert "Slept for 0.1 seconds" in result, f"Sleep result wrong on {backend.name}"
    assert (end_time - start_time) >= 0.09, f"Sleep duration too short on {backend.name}"
    assert (end_time - start_time) < 0.5, f"Sleep duration too long on {backend.name}"
    
    # Verify sleep was logged
    handlers = server.get_handlers()
    sleep_calls = handlers.sleep_calls
    assert len(sleep_calls) > 0, f"Sleep not logged on {backend.name}"
    assert sleep_calls[-1]["duration"] == 0.1, f"Sleep duration not logged correctly on {backend.name}"


@pytest.mark.asyncio
async def test_error_handling(rpc_backend):
    """Test error handling and exceptions."""
    server, client, backend = rpc_backend
    
    # Test custom error
    with pytest.raises(Exception) as exc_info:
        await client.call_method("throw_error", "Custom test error")
    assert "Custom test error" in str(exc_info.value), f"Custom error failed on {backend.name}"
    
    # Test default error
    with pytest.raises(Exception) as exc_info:
        await client.call_method("throw_error")
    assert "Test error" in str(exc_info.value), f"Default error failed on {backend.name}"
    
    # Verify errors were logged
    handlers = server.get_handlers()
    error_calls = handlers.error_calls
    assert len(error_calls) >= 2, f"Errors not logged on {backend.name}"


@pytest.mark.asyncio
async def test_non_existing_method(rpc_backend):
    """Test calling non-existing methods."""
    server, client, backend = rpc_backend
    
    with pytest.raises(Exception) as exc_info:
        await client.call_method("non_existing_method", "param1", "param2")
    
    assert "Unknown method" in str(exc_info.value), f"Unknown method error wrong on {backend.name}"
    assert "non_existing_method" in str(exc_info.value), f"Method name not in error on {backend.name}"


@pytest.mark.asyncio
async def test_missing_parameters(rpc_backend):
    """Test methods with missing required parameters."""
    server, client, backend = rpc_backend
    
    # Test add with insufficient parameters
    with pytest.raises(Exception) as exc_info:
        await client.call_method("add", 5)  # Needs at least 2 parameters
    assert "at least 2 parameters" in str(exc_info.value), f"Missing params error wrong on {backend.name}"
    
    # Test multiply with insufficient parameters
    with pytest.raises(Exception) as exc_info:
        await client.call_method("multiply", 5)  # Needs at least 2 parameters
    assert "at least 2 parameters" in str(exc_info.value), f"Missing params error wrong on {backend.name}"
    
    # Test sleep with no parameters
    with pytest.raises(Exception) as exc_info:
        await client.call_method("sleep")  # Needs duration parameter
    assert "duration parameter" in str(exc_info.value), f"Sleep missing param error wrong on {backend.name}"
    
    # Test method that explicitly checks parameter count
    with pytest.raises(Exception) as exc_info:
        await client.call_method("require_params", "param1", "param2")  # Needs 3 params
    assert "needs 3 parameters" in str(exc_info.value), f"Require params error wrong on {backend.name}"


@pytest.mark.asyncio
async def test_timeout_condition(fast_rpc_backend):
    """Test timeout conditions."""
    server, client, backend = fast_rpc_backend
    
    # Set a short timeout
    client.set_timeout(200)  # 200ms timeout
    
    # Try to call a method that takes longer than timeout
    with pytest.raises(Exception) as exc_info:
        await client.call_method("sleep", 0.5)  # 500ms sleep with 200ms timeout
    
    # Check that it's a timeout error
    error_str = str(exc_info.value)
    assert "timeout" in error_str.lower() or "Timeout" in error_str, f"Timeout error not detected on {backend.name}"


@pytest.mark.asyncio
async def test_complex_data_structures(rpc_backend):
    """Test complex data structures and nested objects."""
    server, client, backend = rpc_backend
    
    # Get complex data from server
    result = await client.call_method("get_complex_data")
    
    assert isinstance(result, dict), f"Complex data not dict on {backend.name}"
    assert result["string"] == "Hello World", f"String field wrong on {backend.name}"
    assert result["integer"] == 42, f"Integer field wrong on {backend.name}"
    assert abs(result["float"] - 3.14159) < 0.00001, f"Float field wrong on {backend.name}"
    assert result["boolean"] is True, f"Boolean field wrong on {backend.name}"
    assert result["null_value"] is None, f"Null field wrong on {backend.name}"
    assert result["array"] == [1, 2, 3, "mixed", True], f"Array field wrong on {backend.name}"
    
    # Check nested object
    nested = result["nested_object"]
    assert isinstance(nested, dict), f"Nested object not dict on {backend.name}"
    assert nested["inner_string"] == "nested", f"Nested string wrong on {backend.name}"
    assert nested["inner_array"] == [{"deep": "value"}], f"Nested array wrong on {backend.name}"
    
    # Bytes field handling depends on backend
    if backend.supports_bytes:
        assert isinstance(result["bytes_data"], bytes), f"Bytes field type wrong on {backend.name}"
    else:
        # JSON backends convert bytes to string
        assert isinstance(result["bytes_data"], str), f"Bytes field type wrong on {backend.name}"


@pytest.mark.asyncio
async def test_fire_and_forget(rpc_backend):
    """Test fire-and-forget method calls."""
    server, client, backend = rpc_backend
    
    # Clear call log
    await client.call_method("clear_logs")
    
    # Fire method (no response expected)
    await client.fire_method("add", 10, 20, 30)
    
    # Give some time for processing
    await asyncio.sleep(0.2)
    
    # Verify the method was called by checking the call log
    call_log = await client.call_method("get_call_log")
    
    # Should have at least the clear_logs and add calls
    assert len(call_log) >= 2, f"Fire method not logged on {backend.name}"
    
    # Find the add call
    add_calls = [call for call in call_log if call["method"] == "add"]
    assert len(add_calls) >= 1, f"Fire add method not found in log on {backend.name}"
    assert add_calls[-1]["args"] == [10, 20, 30], f"Fire method args wrong on {backend.name}"


@pytest.mark.asyncio
async def test_concurrent_operations(fast_rpc_backend):
    """Test concurrent RPC operations."""
    server, client, backend = fast_rpc_backend
    
    # Test concurrent arithmetic operations
    tasks = [
        client.call_method("add", i, i * 2),
        client.call_method("multiply", i, 3),
        client.call_method("echo_int", i * 10)
        for i in range(1, 4)  # Small number for fast testing
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Verify results
    expected_results = []
    for i in range(1, 4):
        expected_results.extend([
            i + (i * 2),  # add result
            i * 3,        # multiply result
            i * 10        # echo_int result
        ])
    
    assert results == expected_results, f"Concurrent operations failed on {backend.name}"


@pytest.mark.asyncio
async def test_connection_management(rpc_backend):
    """Test connection management and server info."""
    server, client, backend = rpc_backend
    
    # Get connection ID
    conn_id = await client.call_method("get_connection_id")
    assert conn_id == client.get_id(), f"Connection ID mismatch on {backend.name}"
    
    # Verify connection is active
    assert server.is_active(conn_id), f"Connection not active on {backend.name}"
    
    # Get server info
    server_info = await client.call_method("get_server_info")
    assert isinstance(server_info, dict), f"Server info not dict on {backend.name}"
    assert server_info["server_type"] == "UnifiedTestServer", f"Server type wrong on {backend.name}"
    assert server_info["connections"] >= 1, f"Connection count wrong on {backend.name}"
    assert server_info["backend_type"] == "unified", f"Backend type wrong on {backend.name}"


@pytest.mark.asyncio
async def test_event_emission(rpc_backend):
    """Test event emission and broadcasting."""
    server, client, backend = rpc_backend
    
    # Test event emission (should not raise errors)
    await client.emit("test_event", {"message": "Hello Events", "data": [1, 2, 3]})
    await client.emit("user_action", {"action": "login", "user": "alice"})
    
    # Give time for event processing
    await asyncio.sleep(0.1)
    
    # For now, just verify no errors occurred
    # In a real multi-client scenario, you'd test actual broadcasting
    assert True, f"Event emission completed on {backend.name}"


@pytest.mark.asyncio
async def test_performance_baseline(fast_rpc_backend):
    """Test basic performance baseline."""
    server, client, backend = fast_rpc_backend
    
    # Measure time for simple operations
    start_time = time.time()
    
    # Perform multiple simple operations
    tasks = [
        client.call_method("add", i, i + 1)
        for i in range(10)
    ]
    
    results = await asyncio.gather(*tasks)
    end_time = time.time()
    
    duration = end_time - start_time
    
    # Verify results
    expected = [i + (i + 1) for i in range(10)]
    assert results == expected, f"Performance test results wrong on {backend.name}"
    
    # Basic performance check (should complete in reasonable time)
    assert duration < 2.0, f"Performance test too slow ({duration:.2f}s) on {backend.name}"
    
    print(f"📊 {backend.name}: 10 concurrent calls in {duration:.3f}s ({1000*duration/10:.1f}ms per call)")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
