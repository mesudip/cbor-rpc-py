import asyncio
import sys

import pytest

from cbor_rpc.stdio.stdio_pipe import StdioPipe
from tests.helpers.stream_pair import create_stream_pair


@pytest.mark.asyncio
async def test_stdio_pipe_errors_without_process():
    server, reader, writer = await create_stream_pair()
    pipe = StdioPipe(reader, writer)

    with pytest.raises(RuntimeError):
        await pipe.wait_for_process_termination()

    # Should not raise
    await pipe.terminate()

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_stdio_pipe_start_process_and_terminate():
    pipe = await StdioPipe.start_process(sys.executable, "-c", "import time; time.sleep(0.2)")
    pipe.terminate()
    code = await pipe.wait_for_process_termination()
    assert isinstance(code, int)


@pytest.mark.asyncio
async def test_stdio_pipe_read_write():
    pipe = await StdioPipe.start_process("/bin/bash", "-c", "cat -")

    received_data = []
    future = asyncio.Future()

    def on_data(data):
        received_data.append(data)
        if len(received_data) == 10:
            future.set_result(None)

    pipe.pipeline("data", on_data)

    test_data = [f"Test data {i}\n".encode("utf-8") for i in range(10)]
    for data in test_data:
        await pipe.write(data)
        await asyncio.sleep(0.01)

    await future

    assert len(received_data) == 10
    for i, (sent, received) in enumerate(zip(test_data, received_data)):
        assert received == sent, f"Mismatch at index {i}: expected {sent!r}, got {received!r}"

    pipe.terminate()
