import pytest
import asyncio
from typing import List
from cbor_rpc import TcpDuplex, TcpServer


@pytest.mark.asyncio
async def test_tcp_client_server_connection():
    """Test basic TCP client-server connection."""
    # Start a server
    server = await TcpServer.create('127.0.0.1', 0)
    server_host, server_port = server.get_address()
    
    connections = []
    
    def on_connection(tcp_duplex: TcpDuplex):
        connections.append(tcp_duplex)
    
    server.on_connection(on_connection)
    
    try:
        # Create a client connection
        client = await TcpDuplex.create_connection(server_host, server_port)
        
        # Wait for server to register the connection
        await asyncio.sleep(0.1)
        
        assert len(connections) == 1
        assert client.is_connected()
        assert connections[0].is_connected()
        
        # Test peer info
        client_peer = client.get_peer_info()
        server_conn_peer = connections[0].get_peer_info()
        
        assert client_peer == (server_host, server_port)
        assert server_conn_peer is not None
        
        await client.terminate()
        
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_tcp_data_exchange():
    """Test bidirectional data exchange over TCP."""
    server = await TcpServer.create('127.0.0.1', 0)
    server_host, server_port = server.get_address()
    
    server_received = []
    client_received = []
    server_connection = None
    
    async def on_connection(tcp_duplex: TcpDuplex):
        nonlocal server_connection
        server_connection = tcp_duplex
        
        async def on_server_data(data: bytes):
            server_received.append(data)
        
        tcp_duplex.on("data", on_server_data)
    
    server.on_connection(on_connection)
    
    try:
        # Create client
        client = await TcpDuplex.create_connection(server_host, server_port)
        
        async def on_client_data(data: bytes):
            client_received.append(data)
        
        client.on("data", on_client_data)
        
        # Wait for connection to be established
        await asyncio.sleep(0.1)
        assert server_connection is not None
        
        # Send data from client to server
        await client.write(b"Hello from client")
        await asyncio.sleep(0.1)
        assert server_received == [b"Hello from client"]
        
        # Send data from server to client
        await server_connection.write(b"Hello from server")
        await asyncio.sleep(0.1)
        assert client_received == [b"Hello from server"]
        
        # Send multiple messages
        await client.write(b"Message 1")
        await client.write(b"Message 2")
        await server_connection.write(b"Response 1")
        await server_connection.write(b"Response 2")
        
        await asyncio.sleep(0.1)
        
        assert b"Message 1" in server_received
        assert b"Message 2" in server_received
        assert b"Response 1" in client_received
        assert b"Response 2" in client_received
        
        await client.terminate()
        
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_tcp_connection_errors():
    """Test TCP connection error handling."""
    # Test connection to non-existent server
    with pytest.raises(ConnectionError):
        await TcpDuplex.create_connection('127.0.0.1', 12345, timeout=0.1)
    
    # Test writing to disconnected client
    client = TcpDuplex()
    with pytest.raises(ConnectionError):
        await client.write(b"test")
    
    # Test double connection
    server = await TcpServer.create('127.0.0.1', 0)
    server_host, server_port = server.get_address()
    
    try:
        client = await TcpDuplex.create_connection(server_host, server_port)
        
        with pytest.raises(ConnectionError):
            await client.connect(server_host, server_port)
        
        await client.terminate()
        
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_tcp_connection_events():
    """Test TCP connection events (connect, close, error)."""
    server = await TcpServer.create('127.0.0.1', 0)
    server_host, server_port = server.get_address()
    
    events = []
    server_connection = None
    
    async def on_connection(tcp_duplex: TcpDuplex):
        nonlocal server_connection
        server_connection = tcp_duplex
        
        async def on_connect():
            events.append("server_connect")
        
        async def on_close(*args):
            events.append(("server_close", args))
        
        async def on_error(error):
            events.append(("server_error", str(error)))
        
        tcp_duplex.on("connect", on_connect)
        tcp_duplex.on("close", on_close)
        tcp_duplex.on("error", on_error)
    
    server.on_connection(on_connection)
    
    try:
        # Create client with event handlers
        client = TcpDuplex()
        
        async def on_client_connect():
            events.append("client_connect")
        
        async def on_client_close(*args):
            events.append(("client_close", args))
        
        async def on_client_error(error):
            events.append(("client_error", str(error)))
        
        client.on("connect", on_client_connect)
        client.on("close", on_client_close)
        client.on("error", on_client_error)
        
        # Connect
        await client.connect(server_host, server_port)
        await asyncio.sleep(0.1)
        
        assert "client_connect" in events
        assert "server_connect" in events
        
        # Close connection
        await client.terminate("test_reason")
        await asyncio.sleep(0.1)
        
        assert ("client_close", ("test_reason",)) in events
        
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_tcp_multiple_connections():
    """Test handling multiple simultaneous TCP connections."""
    server = await TcpServer.create('127.0.0.1', 0)
    server_host, server_port = server.get_address()
    
    connections = []
    
    async def on_connection(tcp_duplex: TcpDuplex):
        connections.append(tcp_duplex)
    
    server.on_connection(on_connection)
    
    try:
        # Create multiple clients
        clients = []
        for i in range(5):
            client = await TcpDuplex.create_connection(server_host, server_port)
            clients.append(client)
        
        await asyncio.sleep(0.1)
        
        # Check that all connections are registered
        assert len(connections) == 5
        assert len(server.get_connections()) == 5
        
        # Close all clients
        for client in clients:
            await client.terminate()
        
        await asyncio.sleep(0.1)
        
        # Check that connections are cleaned up
        assert len(server.get_connections()) == 0
        
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_tcp_large_data_transfer():
    """Test transferring large amounts of data over TCP."""
    server = await TcpServer.create('127.0.0.1', 0)
    server_host, server_port = server.get_address()
    
    received_data = bytearray()
    server_connection = None
    
    async def on_connection(tcp_duplex: TcpDuplex):
        nonlocal server_connection
        server_connection = tcp_duplex
        
        async def on_data(data: bytes):
            received_data.extend(data)
        
        tcp_duplex.on("data", on_data)
    
    server.on_connection(on_connection)
    
    try:
        client = await TcpDuplex.create_connection(server_host, server_port)
        await asyncio.sleep(0.1)
        
        # Send large data (1MB)
        large_data = b"x" * (1024 * 1024)
        await client.write(large_data)
        
        # Wait for all data to be received
        while len(received_data) < len(large_data):
            await asyncio.sleep(0.1)
        
        assert bytes(received_data) == large_data
        
        await client.terminate()
        
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_tcp_server_context_manager():
    """Test using TcpServer as a context manager."""
    async with await TcpServer.create('127.0.0.1', 0) as server:
        server_host, server_port = server.get_address()
        
        client = await TcpDuplex.create_connection(server_host, server_port)
        assert client.is_connected()
        
        await client.terminate()
    
    # Server should be closed automatically


@pytest.mark.asyncio
async def test_tcp_invalid_data_types():
    """Test error handling for invalid data types."""
    server = await TcpServer.create('127.0.0.1', 0)
    server_host, server_port = server.get_address()
    
    try:
        client = await TcpDuplex.create_connection(server_host, server_port)
        
        # Test writing non-bytes data
        with pytest.raises(TypeError):
            await client.write("string data")  # Should be bytes
        
        with pytest.raises(TypeError):
            await client.write(123)  # Should be bytes
        
        # Test writing valid data types
        await client.write(b"bytes data")  # Should work
        await client.write(bytearray(b"bytearray data"))  # Should work
        
        await client.terminate()
        
    finally:
        await server.close()


if __name__ == "__main__":
    pytest.main(["-v", __file__])
