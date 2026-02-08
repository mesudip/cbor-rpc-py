import pytest

from cbor_rpc.tcp.tcp import TcpPipe
from tests.helpers.simple_tcp_server import SimpleTcpServer


@pytest.mark.asyncio
async def test_tcp_pipe_error_paths():
    assert TcpPipe().get_peer_info() is None
    assert TcpPipe().get_local_info() is None

    class BadWriter:
        def get_extra_info(self, _name: str):
            raise RuntimeError("boom")

    pipe = TcpPipe()
    pipe._connected = True
    pipe._writer = BadWriter()
    assert pipe.get_peer_info() is None
    assert pipe.get_local_info() is None

    server = await SimpleTcpServer.create("127.0.0.1", 0)
    host, port = server.get_address()
    await server.stop()

    with pytest.raises(ConnectionError):
        await TcpPipe.create_connection(host, port)

    client, _server_pipe = await TcpPipe.create_inmemory_pair()
    with pytest.raises(ConnectionError):
        await client.connect("127.0.0.1", port)

    await client.terminate()
