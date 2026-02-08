import asyncio
from typing import Any, List

import pytest

from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.rpc.server_base import Server


class DummyServer(Server[EventPipe]):
    def __init__(self, accept_connections: bool = True):
        super().__init__()
        self._accept_connections = accept_connections
        self.stopped = False

    async def start(self, *args, **kwargs) -> Any:
        self._running = True
        return "started"

    async def stop(self) -> None:
        self._running = False
        self.stopped = True

    async def accept(self, pipe: EventPipe) -> bool:
        return self._accept_connections


@pytest.mark.asyncio
async def test_server_base_add_connection_accepts_and_cleans():
    server = DummyServer(accept_connections=True)
    pipe_a, _pipe_b = EventPipe.create_inmemory_pair()

    connected: List[EventPipe] = []

    def on_connection(pipe: EventPipe) -> None:
        connected.append(pipe)

    server.on_connection(on_connection)
    await server._add_connection(pipe_a)
    assert pipe_a in server.get_connections()
    assert connected == [pipe_a]

    await pipe_a.terminate("bye")
    await asyncio.sleep(0)
    assert pipe_a not in server.get_connections()


@pytest.mark.asyncio
async def test_server_base_rejects_connection():
    server = DummyServer(accept_connections=False)
    pipe_a, _pipe_b = EventPipe.create_inmemory_pair()

    await server._add_connection(pipe_a)
    assert pipe_a not in server.get_connections()


@pytest.mark.asyncio
async def test_server_base_close_all_connections_and_context():
    server = DummyServer(accept_connections=True)
    pipe_a, _pipe_b = EventPipe.create_inmemory_pair()
    await server._add_connection(pipe_a)

    await server.close_all_connections()
    await asyncio.sleep(0)
    assert server.get_connections() == set()

    async with server:
        assert server.is_running() is False
    assert server.stopped is True
