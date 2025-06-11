import pytest
import asyncio
from typing import Any, Tuple
from cbor_rpc.pipe.pipe import Pipe
import pytest_asyncio

@pytest_asyncio.fixture
async def pipe_pair():
    pipe1, pipe2 = Pipe.create_pair()
    yield pipe1, pipe2

@pytest.mark.asyncio
async def test_create_pair():
    # Positive case: Creating a pair of sync pipes
    pipe1, pipe2 = Pipe.create_pair()
    assert isinstance(pipe1, Pipe)
    assert isinstance(pipe2, Pipe)
    await pipe1.terminate()
    await pipe2.terminate()

@pytest.mark.asyncio
async def test_write_read(pipe_pair:Tuple[Pipe,Pipe]):
    # Positive case: Writing and reading a chunk successfully
    pipe1, pipe2 = pipe_pair

    assert await pipe1.write("test_chunk") is True
    await asyncio.sleep(0) # Allow event loop to process the write
    assert await pipe2.read() == "test_chunk"

@pytest.mark.asyncio
async def test_close_pipe(pipe_pair:Tuple[Pipe,Pipe]):
    # Positive case: Closing the pipe
    pipe1, pipe2 = pipe_pair
    await pipe1.terminate()

    assert await pipe1.read() is None
    assert pipe1._closed is True
    assert pipe2._closed is True

@pytest.mark.asyncio
async def test_write_after_close(pipe_pair:Tuple[Pipe,Pipe]):
    # Negative case: Writing to a closed pipe
    pipe1, pipe2 = pipe_pair
    await pipe1.terminate()

    assert await pipe1.write("test_chunk") is False

@pytest.mark.asyncio
async def test_read_timeout(pipe_pair):
    # Positive case: Reading with timeout
    pipe1, _ = pipe_pair

    assert await pipe1.read(timeout=0.1) is None

@pytest.mark.asyncio
async def test_bidirectional_communication(pipe_pair:Tuple[Pipe,Pipe]):
    # Positive case: Bidirectional communication between pipes
    pipe1, pipe2 = pipe_pair

    assert await pipe1.write("test_chunk") is True
    await asyncio.sleep(0) # Allow event loop to process the write
    assert await pipe2.read() == "test_chunk"

    assert await pipe2.write("response_chunk") is True
    await asyncio.sleep(0) # Allow event loop to process the write
    assert await pipe1.read() == "response_chunk"

@pytest.mark.asyncio
async def test_parallel_writes(pipe_pair: Tuple[Pipe, Pipe]):
    # Test case: Multiple coroutines writing to one end of the pipe
    pipe1, pipe2 = pipe_pair
    num_writes = 10
    test_chunks = [f"chunk_{i}" for i in range(num_writes)]

    async def writer(chunk):
        return await pipe1.write(chunk)

    # Concurrently write all chunks
    results = await asyncio.gather(*[writer(chunk) for chunk in test_chunks])
    assert all(results)  # All writes should be successful

    # Read all chunks from the other end
    received_chunks = []
    for _ in range(num_writes):
        received_chunks.append(await pipe2.read())

    # Verify all chunks are received and in correct order (or at least all present)
    assert sorted(received_chunks) == sorted(test_chunks)

@pytest.mark.asyncio
async def test_parallel_reads(pipe_pair: Tuple[Pipe, Pipe]):
    # Test case: Multiple coroutines reading from one end of the pipe
    pipe1, pipe2 = pipe_pair
    num_reads = 20
    test_chunks = [f"data_{i}" for i in range(num_reads)]

    # Write all chunks first
    for chunk in test_chunks:
        await pipe1.write(chunk)
        await asyncio.sleep(0) # Allow event loop to process the write

    async def reader():
        return await pipe2.read()

    # Concurrently read all chunks
    received_chunks = await asyncio.gather(*[reader() for _ in range(num_reads)])

    # Verify all chunks are received
    assert sorted(received_chunks) == sorted(test_chunks)

@pytest.mark.asyncio
async def test_concurrent_bidirectional_communication(pipe_pair: Tuple[Pipe, Pipe]):
    # Test case: Concurrent writes and reads from both ends
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

    client_sent, client_received = await client_future
    server_sent, server_received = await server_future

    # Verify client sent messages are received by server
    assert sorted([f"client_msg_{i}" for i in range(num_messages)]) == sorted(server_received)
    
    # Verify server sent messages are received by client
    assert sorted([f"server_response_to_client_msg_{i}" for i in range(num_messages)]) == sorted(client_received)

if __name__ == "__main__":
    pytest.main()
