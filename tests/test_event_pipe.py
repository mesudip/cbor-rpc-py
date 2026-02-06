import pytest
import asyncio
from typing import Any, Tuple
from cbor_rpc import EventPipe
import pytest_asyncio

@pytest_asyncio.fixture
async def event_pipe_pair():
    pipe1, pipe2 = EventPipe.create_inmemory_pair()
    yield pipe1, pipe2
    # Terminate is an async method, so it needs to be awaited
    await pipe1.terminate()
    await pipe2.terminate()

@pytest.mark.asyncio
async def test_create_pair(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Positive case: Creating a pair of async pipes
    pipe1, pipe2 = event_pipe_pair
    assert isinstance(pipe1, EventPipe)
    assert isinstance(pipe2, EventPipe)

@pytest.mark.asyncio
async def test_write_success(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Positive case: Writing a chunk successfully
    pipe1, pipe2 = event_pipe_pair
    result = await pipe1.write("test_chunk")
    assert result is True

@pytest.mark.asyncio
async def test_terminate_success(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Positive case: Terminating the pipe
    pipe1, pipe2 = event_pipe_pair
    await pipe1.terminate()
    # No exception should be raised

@pytest.mark.asyncio
async def test_pipeline_execution(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Positive case: Adding and executing a pipeline
    pipe1, _ = event_pipe_pair
    received_chunk = None
    event = asyncio.Event()

    async def pipeline_handler(chunk: Any) -> None:
        nonlocal received_chunk
        received_chunk = chunk
        event.set()

    pipe1.pipeline("data", pipeline_handler)
    await pipe1._notify("data", "test_chunk")
    await asyncio.wait_for(event.wait(), timeout=1) # Wait for the handler to be called
    assert received_chunk == "test_chunk"

@pytest.mark.asyncio
async def test_pipe_pair(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Positive case: Attaching two pipes
    pipe1, pipe2 = event_pipe_pair
    received_chunk = None
    event = asyncio.Event()

    async def handler(chunk: Any) -> None:
        nonlocal received_chunk
        received_chunk = chunk
        event.set()

    pipe2.pipeline("data", handler)
    await pipe1.write("test_chunk")
    await asyncio.wait_for(event.wait(), timeout=1) # Wait for the handler to be called
    assert received_chunk == "test_chunk"

@pytest.mark.asyncio
async def test_write_after_terminate(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Negative case: Writing to a terminated pipe
    pipe1, _ = event_pipe_pair
    await pipe1.terminate()

    result = await pipe1.write("test_chunk")
    assert result is False

@pytest.mark.asyncio
async def test_parallel_event_writes(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Test case: Multiple coroutines writing to one end of the EventPipe
    pipe1, pipe2 = event_pipe_pair
    num_writes = 10
    test_chunks = [f"chunk_{i}" for i in range(num_writes)]
    received_chunks = []
    
    # Use a lock to protect shared list in concurrent access
    lock = asyncio.Lock() 
    
    # Use a queue to signal when all chunks are received
    received_queue = asyncio.Queue()

    async def pipeline_handler(chunk: Any) -> None:
        async with lock:
            received_chunks.append(chunk)
            if len(received_chunks) == num_writes:
                received_queue.put_nowait(True)

    pipe2.pipeline("data", pipeline_handler)

    async def writer(chunk):
        return await pipe1.write(chunk)

    # Concurrently write all chunks
    results = await asyncio.gather(*[writer(chunk) for chunk in test_chunks])
    assert all(results)  # All writes should be successful

    # Wait for all chunks to be received by the handler
    await asyncio.wait_for(received_queue.get(), timeout=5)

    # Verify all chunks are received
    assert sorted(received_chunks) == sorted(test_chunks)

@pytest.mark.asyncio
async def test_parallel_event_processing(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Test case: One pipe writes multiple chunks, and the other pipe's handler processes them
    pipe1, pipe2 = event_pipe_pair
    num_chunks = 10
    test_chunks = [f"data_{i}" for i in range(num_chunks)]
    processed_chunks = []
    
    lock = asyncio.Lock()
    processed_queue = asyncio.Queue()

    async def processing_handler(chunk: Any) -> None:
        # Simulate some async processing
        await asyncio.sleep(0.01) 
        async with lock:
            processed_chunks.append(chunk)
            if len(processed_chunks) == num_chunks:
                processed_queue.put_nowait(True)

    pipe2.pipeline("data", processing_handler)

    # Write all chunks sequentially (or in parallel, EventPipe handles internal queueing)
    write_tasks = [pipe1.write(chunk) for chunk in test_chunks]
    await asyncio.gather(*write_tasks)

    # Wait for all chunks to be processed
    await asyncio.wait_for(processed_queue.get(), timeout=5)

    # Verify all chunks are processed
    assert sorted(processed_chunks) == sorted(test_chunks)

@pytest.mark.asyncio
async def test_concurrent_bidirectional_event_communication(event_pipe_pair: Tuple[EventPipe, EventPipe]):
    # Test case: Concurrent writes and event processing from both ends
    pipe1, pipe2 = event_pipe_pair
    num_messages = 5
    
    client_sent_msgs = []
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

    pipe1.pipeline("data", client_handler) # Client listens for responses on the 'data' pipeline
    pipe2.pipeline("data", server_handler)    # Server listens for client messages

    async def client_writer_task():
        for i in range(num_messages):
            msg = f"client_msg_{i}"
            await pipe1.write(msg)
            client_sent_msgs.append(msg)
            # Client expects a response, but it's handled by client_handler

    asyncio.create_task(client_writer_task())

    # Wait for both sides to complete their communication
    await asyncio.wait_for(asyncio.gather(client_done_event.wait(), server_done_event.wait()), timeout=5)

    # Verify client sent messages are received by server
    assert sorted([f"client_msg_{i}" for i in range(num_messages)]) == sorted(server_received_msgs)
    
    # Verify server sent messages are received by client
    assert sorted([f"server_response_to_client_msg_{i}" for i in range(num_messages)]) == sorted(client_received_responses)

if __name__ == "__main__":
    pytest.main()
