"""
Example of how to add custom backend types to the test suite.
This demonstrates the extensibility of the RPC testing framework.
"""

import pytest
import asyncio
from typing import Any, List
from cbor_rpc import Pipe, JsonTransformer
from .test_rpc_generic import BaseTestRpcServer, JsonTestRpcServer
from .helpers.backend_factory import BackendFactory, create_json_backend


# Example 1: Custom server with special behavior
class EchoOnlyTestServer(BaseTestRpcServer):
    """A test server that only handles echo methods."""
    
    async def _handle_default_method(self, connection_id: str, method: str, args: List[Any]) -> Any:
        if method.startswith("echo"):
            return {"echoed": args, "server": "EchoOnlyTestServer"}
        else:
            raise Exception(f"EchoOnlyTestServer only handles echo methods, got: {method}")


# Example 2: Custom transformer (mock compression)
class MockCompressionTransformer(Pipe[Any, Any]):
    """Mock compression transformer for demonstration."""
    
    def __init__(self, underlying_pipe: Pipe[Any, Any]):
        super().__init__()
        self.underlying_pipe = underlying_pipe
        self._closed = False
        
        # Forward events from underlying pipe
        async def on_underlying_data(data: Any):
            # Mock decompression: just add a marker
            if isinstance(data, dict):
                data["_decompressed"] = True
            await self._emit("data", data)
        
        async def on_underlying_close(*args):
            await self._emit("close", *args)
        
        async def on_underlying_error(error):
            await self._emit("error", error)
        
        self.underlying_pipe.on("data", on_underlying_data)
        self.underlying_pipe.on("close", on_underlying_close)
        self.underlying_pipe.on("error", on_underlying_error)
    
    async def write(self, chunk: Any) -> bool:
        """Write data after mock compression."""
        if self._closed:
            return False
        
        # Mock compression: just add a marker
        if isinstance(chunk, dict):
            chunk["_compressed"] = True
        
        return await self.underlying_pipe.write(chunk)
    
    async def terminate(self, *args: Any) -> None:
        """Terminate the underlying pipe."""
        if self._closed:
            return
        self._closed = True
        await self.underlying_pipe.terminate(*args)


# Create custom backend configurations
def create_echo_only_backend():
    """Create a backend with echo-only server."""
    return BackendFactory.create_simple_backend(
        name="echo-only",
        description="Backend with echo-only server",
        server_class=EchoOnlyTestServer
    )


def create_compression_backend():
    """Create a backend with mock compression."""
    return BackendFactory.create_transformer_backend(
        name="mock-compression",
        description="Backend with mock compression transformer",
        transformer_class=MockCompressionTransformer
    )


def create_json_compression_backend():
    """Create a backend with both JSON and compression."""
    def double_transformer_factory():
        # Create base pipes
        pipe1, pipe2 = Pipe.create_pair()
        
        # Apply JSON transformation first
        json_pipe1 = JsonTransformer(pipe1)
        json_pipe2 = JsonTransformer(pipe2)
        
        # Then apply compression
        compressed_pipe1 = MockCompressionTransformer(json_pipe1)
        compressed_pipe2 = MockCompressionTransformer(json_pipe2)
        
        return compressed_pipe1, compressed_pipe2
    
    return BackendFactory.create_simple_backend(
        name="json-compression",
        description="Backend with JSON + compression transformers",
        server_class=JsonTestRpcServer,
        pipe_factory=double_transformer_factory
    )


# Custom backend configurations for testing
CUSTOM_BACKENDS = [
    create_echo_only_backend(),
    create_compression_backend(),
    create_json_compression_backend(),
]


@pytest.fixture(params=CUSTOM_BACKENDS)
async def custom_backend(request):
    """Fixture for testing custom backends."""
    backend = request.param
    
    print(f"\n🔧 Custom testing with {backend.name}: {backend.description}")
    
    server, client = await backend.create_server_client_pair()
    await asyncio.sleep(0.1)
    
    try:
        yield server, client, backend
    finally:
        await backend.cleanup_backend(server, client)


# Tests for custom backends
@pytest.mark.asyncio
async def test_echo_only_server(custom_backend):
    """Test that demonstrates backend-specific behavior."""
    server, client, backend = custom_backend
    
    if backend.name == "echo-only":
        # This should work
        result = await client.call_method("echo", "test message")
        assert "echoed" in result
        assert result["server"] == "EchoOnlyTestServer"
        
        # This should fail
        with pytest.raises(Exception) as exc_info:
            await client.call_method("add", 1, 2, 3)
        assert "only handles echo methods" in str(exc_info.value)


@pytest.mark.asyncio
async def test_compression_markers(custom_backend):
    """Test that compression markers are added."""
    server, client, backend = custom_backend
    
    if "compression" in backend.name:
        # Test that compression/decompression markers are present
        result = await client.call_method("echo", {"test": "data"})
        
        # The result should have decompression marker
        if isinstance(result, dict):
            # For simple compression backend
            if backend.name == "mock-compression":
                assert result.get("_decompressed") is True
            # For JSON+compression backend, the structure might be different
            elif backend.name == "json-compression":
                # JSON transformation might change the structure
                pass


@pytest.mark.asyncio
async def test_json_compression_combination(custom_backend):
    """Test JSON + compression combination."""
    server, client, backend = custom_backend
    
    if backend.name == "json-compression":
        # Test complex JSON data with compression
        complex_data = {
            "users": [{"name": "Alice"}, {"name": "Bob"}],
            "metadata": {"version": "1.0"}
        }
        
        result = await client.call_method("echo", complex_data)
        assert result == complex_data  # Should maintain data integrity


# Example of how to run specific backend tests
@pytest.mark.asyncio
async def test_specific_backend_only():
    """Example of testing a specific backend configuration."""
    # Create a specific backend for targeted testing
    backend = create_json_backend("test-json", "Test JSON backend")
    
    server, client = await backend.create_server_client_pair()
    
    try:
        # Test JSON-specific functionality
        result = await client.call_method("json_echo", "Hello JSON!")
        assert isinstance(result, dict)
        assert result["echoed"] == "Hello JSON!"
        
    finally:
        await backend.cleanup_backend(server, client)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
