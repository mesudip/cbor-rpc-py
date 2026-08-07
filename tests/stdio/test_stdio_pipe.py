import asyncio
import os
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
    await pipe.terminate()
    code = await pipe.wait_for_process_termination()
    assert isinstance(code, int)


@pytest.mark.asyncio
async def test_stdio_pipe_read_write():
    pipe = await StdioPipe.start_process("/bin/bash", "-c", "cat -")

    received_data = []
    future = asyncio.Future()
    expected_payload = b"".join([f"Test data {i}\n".encode("utf-8") for i in range(10)])

    def on_data(data):
        received_data.append(data)
        if b"".join(received_data) == expected_payload:
            future.set_result(None)

    pipe.pipeline("data", on_data)

    test_data = [f"Test data {i}\n".encode("utf-8") for i in range(10)]
    for data in test_data:
        await pipe.write(data)
        await asyncio.sleep(0.01)

    await future

    assert b"".join(received_data) == expected_payload

    await pipe.terminate()


@pytest.mark.asyncio
async def test_stdio_pipe_start_process_supports_cwd_and_env():
    target_cwd = "/tmp"
    pipe = await StdioPipe.start_process(
        sys.executable,
        "-c",
        "import os,sys; sys.stdout.write(os.getcwd() + '|' + os.environ.get('CBOR_RPC_TEST_ENV', ''))",
        cwd=target_cwd,
        env={**os.environ, "CBOR_RPC_TEST_ENV": "ok"},
    )
    data = await pipe._reader.read(1024)
    await pipe.terminate()
    actual_cwd, actual_env = data.decode("utf-8").split("|", 1)
    assert os.path.realpath(actual_cwd) == os.path.realpath(target_cwd)
    assert actual_env == "ok"


@pytest.mark.asyncio
async def test_stdio_pipe_start_process_stderr_pipe_mode_emits_stderr():
    pipe = await StdioPipe.start_process(
        sys.executable,
        "-c",
        "import sys; sys.stderr.write('boom\\n')",
        stderr_mode="pipe",
    )

    stderr_future = asyncio.Future()

    async def on_stderr(data: bytes):
        if not stderr_future.done():
            stderr_future.set_result(data)

    pipe.on("stderr", on_stderr)
    stderr_data = await asyncio.wait_for(stderr_future, timeout=2)
    await pipe.terminate()
    assert b"boom" in stderr_data
