import asyncio
from typing import Tuple

import pytest
import pytest_asyncio

from cbor_rpc.pipe.pipe import Pipe


@pytest_asyncio.fixture
async def pipe_pair():
    pipe1, pipe2 = Pipe.create_pair()
    yield pipe1, pipe2


@pytest.mark.asyncio
async def test_create_pair():
    pipe1, pipe2 = Pipe.create_pair()
    assert isinstance(pipe1, Pipe)
    assert isinstance(pipe2, Pipe)
    await pipe1.terminate()
    await pipe2.terminate()


@pytest.mark.asyncio
async def test_write_read(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, pipe2 = pipe_pair

    assert await pipe1.write("test_chunk") is True
    await asyncio.sleep(0)
    assert await pipe2.read() == "test_chunk"


@pytest.mark.asyncio
async def test_close_pipe(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, pipe2 = pipe_pair
    await pipe1.terminate()

    assert await pipe1.read() is None
    assert pipe1._closed is True
    assert pipe2._closed is True


@pytest.mark.asyncio
async def test_write_after_close(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, _pipe2 = pipe_pair
    await pipe1.terminate()

    assert await pipe1.write("test_chunk") is False


@pytest.mark.asyncio
async def test_read_timeout(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, _pipe2 = pipe_pair

    assert await pipe1.read(timeout=0.1) is None


@pytest.mark.asyncio
async def test_bidirectional_communication(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, pipe2 = pipe_pair

    assert await pipe1.write("test_chunk") is True
    await asyncio.sleep(0)
    assert await pipe2.read() == "test_chunk"

    assert await pipe2.write("response_chunk") is True
    await asyncio.sleep(0)
    assert await pipe1.read() == "response_chunk"


@pytest.mark.asyncio
async def test_parallel_writes(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, pipe2 = pipe_pair
    num_writes = 10
    test_chunks = [f"chunk_{i}" for i in range(num_writes)]

    async def writer(chunk):
        return await pipe1.write(chunk)

    results = await asyncio.gather(*[writer(chunk) for chunk in test_chunks])
    assert all(results)

    received_chunks = []
    for _ in range(num_writes):
        received_chunks.append(await pipe2.read())

    assert sorted(received_chunks) == sorted(test_chunks)


@pytest.mark.asyncio
async def test_parallel_reads(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, pipe2 = pipe_pair
    num_reads = 20
    test_chunks = [f"data_{i}" for i in range(num_reads)]

    for chunk in test_chunks:
        await pipe1.write(chunk)
        await asyncio.sleep(0)

    async def reader():
        return await pipe2.read()

    received_chunks = await asyncio.gather(*[reader() for _ in range(num_reads)])

    assert sorted(received_chunks) == sorted(test_chunks)


@pytest.mark.asyncio
async def test_concurrent_bidirectional_communication(pipe_pair: Tuple[Pipe, Pipe]):
    pipe1, pipe2 = pipe_pair
    num_messages = 20

    async def client_task():
        sent = []
        received = []
        for i in range(num_messages):
            msg = f"client_msg_{i}"
            await pipe1.write(msg)
            sent.append(msg)
            response = await pipe1.read()
            received.append(response)
        return sent, received

    async def server_task():
        sent = []
        received = []
        for i in range(num_messages):
            msg = await pipe2.read()
            received.append(msg)
            response = f"server_response_to_{msg}"
            await pipe2.write(response)
            sent.append(response)
        return sent, received

    client_future = asyncio.create_task(client_task())
    server_future = asyncio.create_task(server_task())

    _client_sent, client_received = await client_future
    _server_sent, server_received = await server_future

    assert sorted([f"client_msg_{i}" for i in range(num_messages)]) == sorted(server_received)
    assert sorted([f"server_response_to_client_msg_{i}" for i in range(num_messages)]) == sorted(client_received)
