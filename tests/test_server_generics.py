"""
Tests for the generic Server class and its type safety.
"""

import pytest
import asyncio
from typing import Any, Set
from cbor_rpc import Server, EventPipe, TcpServer, TcpPipe


class MockPipe(EventPipe[str, str]):
    """A mock pipe for testing."""
    
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self._closed = False
    
    async def write(self, chunk: str) -> bool:
        if self._closed:
            return False
        await self._emit("data", chunk)
        return True
    
    async def terminate(self, *args: Any) -> None:
        if self._closed:
            return
        self._closed = True
        await self._emit("close", *args)


class MockServer(Server[MockPipe]):
    """A mock server for testing generic functionality."""
    
    def __init__(self):
        super().__init__()
        self._started = False
    
    async def start(self) -> str:
        self._started = True
        self._running = True
        return "mock-server-started"
    
    async def stop(self) -> None:
        if not self._running:
            return
        self._started = False
        self._running = False
        await self.close_all_connections()
    
    async def add_mock_connection(self, pipe: MockPipe) -> None:
        """Add a mock connection for testing."""
        await self._add_connection(pipe)


@pytest.mark.asyncio
async def test_generic_server_typing():
    """Test that Server generic typing works correctly."""
    server: Server[MockPipe] = MockServer()
    
    # Test server initialization
    assert not server.is_running()
    assert len(server.get_connections()) == 0
    
    # Start server
    result = await server.start()
    assert result == "mock-server-started"
    assert server.is_running()
    
    # Test connection handling
    connection_events = []
    
    def on_connection(pipe: MockPipe):
        connection_events.append(pipe)
        assert isinstance(pipe, MockPipe)
    
    server.on_connection(on_connection)
    
    # Add mock connections
    pipe1 = MockPipe("test-pipe-1")
    pipe2 = MockPipe("test-pipe-2")
    
    await server.add_mock_connection(pipe1)
    await server.add_mock_connection(pipe2)
    
    # Verify connections were added
    assert len(connection_events) == 2
    assert connection_events[0] == pipe1
    assert connection_events[1] == pipe2
    
    # Verify get_connections returns correct type
    connections: Set[MockPipe] = server.get_connections()
    assert len(connections) == 2
    assert pipe1 in connections
    assert pipe2 in connections
    
    # Test connection cleanup on close
    await pipe1.terminate()
    await asyncio.sleep(0.01)  # Allow cleanup
    
    connections = server.get_connections()
    assert len(connections) == 1
    assert pipe2 in connections
    assert pipe1 not in connections
    
    # Stop server
    await server.stop()
    assert not server.is_running()
    assert len(server.get_connections()) == 0


@pytest.mark.asyncio
async def test_tcp_server_generic_typing():
    """Test that TcpServer properly implements Server[TcpPipe]."""
    # Create TCP server
    tcp_server: Server[TcpPipe] = await TcpServer.create('127.0.0.1', 0)
    
    # Verify it's the correct type
    assert isinstance(tcp_server, Server)
    assert isinstance(tcp_server, TcpServer)
    
    # Test connection event typing
    connection_events = []
    
    def on_tcp_connection(pipe: TcpPipe):
        connection_events.append(pipe)
        # Verify the pipe is the correct type
        assert isinstance(pipe, TcpPipe)
        assert isinstance(pipe, EventPipe)
    
    tcp_server.on_connection(on_tcp_connection)
    
    try:
        # Create client connection
        host, port = tcp_server.get_address()
        client = await TcpPipe.create_connection(host, port)
        
        await asyncio.sleep(0.1)
        
        # Verify connection event was emitted with correct type
        assert len(connection_events) == 1
        assert isinstance(connection_events[0], TcpPipe)
        
        # Verify get_connections returns TcpPipe instances
        connections: Set[TcpPipe] = tcp_server.get_connections()
        assert len(connections) == 1
        for conn in connections:
            assert isinstance(conn, TcpPipe)
        
        await client.terminate()
        
    finally:
        await tcp_server.stop()


@pytest.mark.asyncio
async def test_server_context_manager():
    """Test server context manager functionality."""
    async with MockServer() as server:
        await server.start()
        assert server.is_running()
        
        pipe = MockPipe("context-test")
        await server.add_mock_connection(pipe)
        assert len(server.get_connections()) == 1
    
    # Server should be stopped automatically
    assert not server.is_running()


@pytest.mark.asyncio
async def test_server_close_all_connections():
    """Test that close_all_connections properly closes all connections."""
    server = MockServer()
    await server.start()
    
    # Add multiple connections
    pipes = [MockPipe(f"pipe-{i}") for i in range(3)]
    for pipe in pipes:
        await server.add_mock_connection(pipe)
    
    assert len(server.get_connections()) == 3
    
    # Close all connections
    await server.close_all_connections()
    await asyncio.sleep(0.01)  # Allow cleanup
    
    # All connections should be closed
    assert len(server.get_connections()) == 0
    
    await server.stop()


if __name__ == "__main__":
    pytest.main(["-v", __file__])
