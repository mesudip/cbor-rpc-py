import asyncio
from typing import Any, Tuple

import pytest
import pytest_asyncio

from cbor_rpc import EventPipe


@pytest_asyncio.fixture
async def event_pipe_pair():
    pipe1, pipe2 = EventPipe.create_inmemory_pair()
    yield pipe1, pipe2
    await pipe1.terminate()
    await pipe2.terminate()


@pytest.mark.asyncio
async def test_create_pair(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, pipe2 = event_pipe_pair
    assert isinstance(pipe1, EventPipe)
    assert isinstance(pipe2, EventPipe)


@pytest.mark.asyncio
async def test_write_success(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, _pipe2 = event_pipe_pair
    result = await pipe1.write("test_chunk")
    assert result is True


@pytest.mark.asyncio
async def test_terminate_success(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, _pipe2 = event_pipe_pair
    await pipe1.terminate()


@pytest.mark.asyncio
async def test_pipeline_execution(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, _pipe2 = event_pipe_pair
    received_chunk = None
    event = asyncio.Event()

    async def pipeline_handler(chunk: Any) -> None:
        nonlocal received_chunk
        received_chunk = chunk
        event.set()

    pipe1.pipeline("data", pipeline_handler)
    await pipe1._notify("data", "test_chunk")
    await asyncio.wait_for(event.wait(), timeout=1)
    assert received_chunk == "test_chunk"


@pytest.mark.asyncio
async def test_pipe_pair(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, pipe2 = event_pipe_pair
    received_chunk = None
    event = asyncio.Event()

    async def handler(chunk: Any) -> None:
        nonlocal received_chunk
        received_chunk = chunk
        event.set()

    pipe2.pipeline("data", handler)
    await pipe1.write("test_chunk")
    await asyncio.wait_for(event.wait(), timeout=1)
    assert received_chunk == "test_chunk"


@pytest.mark.asyncio
async def test_write_after_terminate(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, _pipe2 = event_pipe_pair
    await pipe1.terminate()

    result = await pipe1.write("test_chunk")
    assert result is False


@pytest.mark.asyncio
async def test_parallel_event_writes(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, pipe2 = event_pipe_pair
    num_writes = 10
    test_chunks = [f"chunk_{i}" for i in range(num_writes)]
    received_chunks = []

    lock = asyncio.Lock()
    received_queue = asyncio.Queue()

    async def pipeline_handler(chunk: Any) -> None:
        async with lock:
            received_chunks.append(chunk)
            if len(received_chunks) == num_writes:
                received_queue.put_nowait(True)

    pipe2.pipeline("data", pipeline_handler)

    async def writer(chunk):
        return await pipe1.write(chunk)

    results = await asyncio.gather(*[writer(chunk) for chunk in test_chunks])
    assert all(results)

    await asyncio.wait_for(received_queue.get(), timeout=5)

    assert sorted(received_chunks) == sorted(test_chunks)


@pytest.mark.asyncio
async def test_parallel_event_processing(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    pipe1, pipe2 = event_pipe_pair
    num_chunks = 10
    test_chunks = [f"data_{i}" for i in range(num_chunks)]
    processed_chunks = []

    lock = asyncio.Lock()
    processed_queue = asyncio.Queue()

    async def processing_handler(chunk: Any) -> None:
        await asyncio.sleep(0.01)
        async with lock:
            processed_chunks.append(chunk)
            if len(processed_chunks) == num_chunks:
                processed_queue.put_nowait(True)

    pipe2.pipeline("data", processing_handler)

    write_tasks = [pipe1.write(chunk) for chunk in test_chunks]
    await asyncio.gather(*write_tasks)

    await asyncio.wait_for(processed_queue.get(), timeout=5)

    assert sorted(processed_chunks) == sorted(test_chunks)


@pytest.mark.asyncio
async def test_concurrent_bidirectional_event_communication(
    event_pipe_pair: Tuple[EventPipe, EventPipe],
):
    pipe1, pipe2 = event_pipe_pair
    num_messages = 5

    client_received_responses = []
    server_received_msgs = []
    server_sent_responses = []

    client_done_event = asyncio.Event()
    server_done_event = asyncio.Event()

    async def client_handler(response: Any) -> None:
        client_received_responses.append(response)
        if len(client_received_responses) == num_messages:
            client_done_event.set()

    async def server_handler(msg: Any) -> None:
        server_received_msgs.append(msg)
        response = f"server_response_to_{msg}"
        await pipe2.write(response)
        server_sent_responses.append(response)
        if len(server_sent_responses) == num_messages:
            server_done_event.set()

    pipe1.pipeline("data", client_handler)
    pipe2.pipeline("data", server_handler)

    async def client_writer_task():
        for i in range(num_messages):
            msg = f"client_msg_{i}"
            await pipe1.write(msg)

    asyncio.create_task(client_writer_task())

    await asyncio.wait_for(asyncio.gather(client_done_event.wait(), server_done_event.wait()), timeout=5)

    assert sorted([f"client_msg_{i}" for i in range(num_messages)]) == sorted(server_received_msgs)
    assert sorted([f"server_response_to_client_msg_{i}" for i in range(num_messages)]) == sorted(
        client_received_responses
    )
