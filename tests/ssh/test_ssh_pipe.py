import pytest

from cbor_rpc.ssh.ssh_pipe import SshPipe
from tests.helpers.stream_pair import create_stream_pair


@pytest.mark.asyncio
async def test_ssh_pipe_terminate_and_write_eof():
    server, reader, writer = await create_stream_pair()

    class DummyChannel:
        def __init__(self):
            self._closed = False

        def is_closing(self) -> bool:
            return self._closed

        async def wait_closed(self) -> None:
            return None

        def terminate(self) -> None:
            self._closed = True

    pipe = SshPipe(reader, writer, DummyChannel())

    await pipe.write_eof()
    await pipe.terminate()

    server.close()
    await server.wait_closed()
