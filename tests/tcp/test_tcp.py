import asyncio
from typing import List

import pytest

from cbor_rpc import TcpPipe
from tests.helpers.simple_tcp_server import SimpleTcpServer

DEFAULT_TIMEOUT = 2.0


@pytest.mark.asyncio
async def test_tcp_client_server_connection():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    connections = []

    def on_connection(tcp_pipe: TcpPipe):
        connections.append(tcp_pipe)

    server.on_connection(on_connection)

    try:
        client = await TcpPipe.create_connection(server_host, server_port)
        await asyncio.sleep(0.1)

        assert len(connections) == 1
        assert client.is_connected()
        assert connections[0].is_connected()

        client_peer = client.get_peer_info()
        server_conn_peer = connections[0].get_peer_info()

        assert client_peer == (server_host, server_port)
        assert server_conn_peer is not None

        await client.terminate()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_data_exchange():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    server_received = []
    client_received = []
    server_connection = None

    async def on_connection(tcp_pipe: TcpPipe):
        nonlocal server_connection
        server_connection = tcp_pipe

        async def on_server_data(data: bytes):
            server_received.append(data)

        tcp_pipe.on("data", on_server_data)

    server.on_connection(on_connection)

    try:
        client = await TcpPipe.create_connection(server_host, server_port)

        async def on_client_data(data: bytes):
            client_received.append(data)

        client.on("data", on_client_data)

        await asyncio.sleep(0.1)
        assert server_connection is not None

        await client.write(b"Hello from client")
        await asyncio.sleep(0.1)
        assert server_received == [b"Hello from client"]

        await server_connection.write(b"Hello from server")
        await asyncio.sleep(0.1)
        assert client_received == [b"Hello from server"]

        server_received.clear()
        client_received.clear()

        await client.write(b"Message 1")
        await asyncio.sleep(0.05)
        await client.write(b"Message 2")
        await asyncio.sleep(0.1)

        await server_connection.write(b"Response 1")
        await asyncio.sleep(0.05)
        await server_connection.write(b"Response 2")
        await asyncio.sleep(0.1)

        server_data = b"".join(server_received)
        client_data = b"".join(client_received)

        assert b"Message 1" in server_data
        assert b"Message 2" in server_data
        assert b"Response 1" in client_data
        assert b"Response 2" in client_data

        await client.terminate()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_connection_errors():
    with pytest.raises(ConnectionError):
        await TcpPipe.create_connection("127.0.0.1", 12345, timeout=DEFAULT_TIMEOUT)

    client = TcpPipe()
    with pytest.raises(ConnectionError):
        await client.write(b"test")

    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    try:
        client = await TcpPipe.create_connection(server_host, server_port)

        with pytest.raises(ConnectionError):
            await client.connect(server_host, server_port)

        await client.terminate()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_connection_events():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    events = []

    server.on_connection(lambda conn: events.append("server_connect"))

    try:
        client = TcpPipe()

        async def on_client_connect():
            events.append("client_connect")

        async def on_client_close(*args):
            events.append(("client_close", args))

        async def on_client_error(error):
            events.append(("client_error", str(error)))

        client.on("connect", on_client_connect)
        client.on("close", on_client_close)
        client.on("error", on_client_error)

        await client.connect(server_host, server_port)
        await asyncio.sleep(0.2)

        assert "client_connect" in events
        assert "server_connect" in events

        events_before_close = len(events)
        await client.terminate("test_reason")
        await asyncio.sleep(0.2)

        assert len(events) > events_before_close
        close_events = [e for e in events if isinstance(e, tuple) and e[0] == "client_close"]
        assert len(close_events) > 0

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_client_connection_tracking():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    try:
        clients: List[TcpPipe] = []
        server.on_connection(lambda conn: print(f"New connection: {conn}"))
        for i in range(5):
            client = await TcpPipe.create_connection(server_host, server_port)
            client.on("close", lambda: print(f"Connection[{i}] closed"))
            clients.append(client)

        await asyncio.sleep(0.5)

        assert len(server.get_connections()) == 5

        for client in clients:
            await client.terminate()

        await asyncio.sleep(0.2)

        assert len(server.get_connections()) == 0, "Connections not clean uped"

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_client_connection_tracking_self():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    try:
        clients: List[TcpPipe] = []
        server.on_connection(lambda conn: print(f"New connection: {conn}"))
        for i in range(5):
            client = await TcpPipe.create_connection(server_host, server_port)
            client.on("close", lambda: print(f"Connection[{i}] closed"))
            clients.append(client)

        await asyncio.sleep(0.5)

        assert len(server.get_connections()) == 5

        for duplex in server.get_connections():
            await duplex.terminate()

        await asyncio.sleep(0.2)

        assert len(server.get_connections()) == 0, "Connections not clean uped"

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_large_data_transfer():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    received_data = bytearray()
    server_connection = None

    async def on_connection(tcp_pipe: TcpPipe):
        nonlocal server_connection
        server_connection = tcp_pipe

        async def on_data(data: bytes):
            received_data.extend(data)

        tcp_pipe.on("data", on_data)

    server.on_connection(on_connection)

    try:
        client = await TcpPipe.create_connection(server_host, server_port)
        await asyncio.sleep(0.1)

        large_data = b"x" * (100 * 1024 * 1024)
        await client.write(large_data)

        timeout = 5.0
        start_time = asyncio.get_event_loop().time()
        while len(received_data) < len(large_data):
            if asyncio.get_event_loop().time() - start_time > timeout:
                break
            await asyncio.sleep(0.1)

        assert bytes(received_data) == large_data

        await client.terminate()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_server_context_manager():
    async with await SimpleTcpServer.create("127.0.0.1", 0) as server:
        server_host, server_port = server.get_address()

        client = await TcpPipe.create_connection(server_host, server_port)
        assert client.is_connected()

        await client.terminate()


@pytest.mark.asyncio
async def test_tcp_invalid_data_types():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    try:
        client = await TcpPipe.create_connection(server_host, server_port)

        with pytest.raises(TypeError):
            await client.write("string data")

        with pytest.raises(TypeError):
            await client.write(123)

        await client.write(b"bytes data")
        await client.write(bytearray(b"bytearray data"))

        await client.terminate()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_inmemory_pair_bidirectional_exchange():
    client_pipe, server_pipe = await TcpPipe.create_inmemory_pair()

    try:
        assert client_pipe.is_connected()
        assert server_pipe.is_connected()

        client_received = asyncio.Queue()
        server_received = asyncio.Queue()

        client_pipe.on("data", client_received.put_nowait)
        server_pipe.on("data", server_received.put_nowait)

        await client_pipe.write(b"ping")
        server_data = await asyncio.wait_for(server_received.get(), timeout=DEFAULT_TIMEOUT)
        assert server_data == b"ping"

        await server_pipe.write(b"pong")
        client_data = await asyncio.wait_for(client_received.get(), timeout=DEFAULT_TIMEOUT)
        assert client_data == b"pong"

    finally:
        await client_pipe.terminate()
        await server_pipe.terminate()


@pytest.mark.asyncio
async def test_tcp_shutdown_keeps_active_connections():
    server = await SimpleTcpServer.create("127.0.0.1", 0)
    server_host, server_port = server.get_address()

    server_connection = None

    async def on_connection(tcp_pipe: TcpPipe):
        nonlocal server_connection
        server_connection = tcp_pipe

    server.on_connection(on_connection)

    try:
        client = await TcpPipe.create_connection(server_host, server_port)
        await asyncio.wait_for(asyncio.sleep(0.1), timeout=DEFAULT_TIMEOUT)
        assert server_connection is not None

        await server.shutdown()

        client_received = asyncio.Queue()
        server_received = asyncio.Queue()

        client.on("data", client_received.put_nowait)
        server_connection.on("data", server_received.put_nowait)

        await client.write(b"still-alive")
        server_data = await asyncio.wait_for(server_received.get(), timeout=DEFAULT_TIMEOUT)
        assert server_data == b"still-alive"

        await server_connection.write(b"still-alive-2")
        client_data = await asyncio.wait_for(client_received.get(), timeout=DEFAULT_TIMEOUT)
        assert client_data == b"still-alive-2"

        with pytest.raises(ConnectionError) as exc_info:
            await TcpPipe.create_connection(server_host, server_port, timeout=0.2)
        error_text = str(exc_info.value).lower()
        assert "refused" in error_text or "connect call failed" in error_text

        await client.terminate()
        await server_connection.terminate()

    finally:
        await server.stop()
