import pytest
import asyncio
from typing import Any
from cbor_rpc import Pipe, AbstractEmitter

@pytest.fixture
def pipe():
    # Create a simple pipe for testing
    def writer(chunk: Any) -> None:
        pass
    
    def terminator(*args: Any) -> None:
        pass
    
    return Pipe.make_pipe(writer, terminator)

@pytest.mark.asyncio
async def test_write_success(pipe):
    # Positive case: Writing a chunk successfully
    result = await pipe.write("test_chunk")
    assert result is True

@pytest.mark.asyncio
async def test_terminate_success(pipe):
    # Positive case: Terminating the pipe
    await pipe.terminate()
    # No exception should be raised

@pytest.mark.asyncio
async def test_on_event_subscription(pipe):
    # Positive case: Subscribing to an event
    called = False
    def handler(chunk: Any) -> None:
        nonlocal called
        called = True
    
    pipe.on("data", handler)
    await pipe._notify("data", "test_chunk")
    assert called is True

@pytest.mark.asyncio
async def test_pipeline_execution(pipe):
    # Positive case: Adding and executing a pipeline
    called = False
    def pipeline_handler(chunk: Any) -> None:
        nonlocal called
        called = True
    
    pipe.pipeline("data", pipeline_handler)
    await pipe._notify("data", "test_chunk")
    assert called is True

@pytest.mark.asyncio
async def test_async_data_handler(pipe):
    # Positive case: Handling async event handlers
    called = False
    async def async_handler(chunk: Any) -> None:
        nonlocal called
        called = True
    
    pipe.on("data", async_handler)
    await pipe.write("something")
    assert called is True

@pytest.mark.asyncio
async def test_sync_data_handler(pipe):
    # Positive case: Handling async event handlers
    called = False
    def sync_handler(chunk: Any) -> None:
        nonlocal called
        called = True
    
    pipe.on("data", sync_handler)
    await pipe.write("something")
    assert called is True

@pytest.mark.asyncio
async def test_attach_pipes():
    # Positive case: Attaching two pipes
    pipe1 = Pipe.make_pipe(lambda x: None, lambda: None)
    pipe2 = Pipe.make_pipe(lambda x: None, lambda: None)
    
    Pipe.attach(pipe1, pipe2)
    
    called = False
    def handler(chunk: Any) -> None:
        nonlocal called
        called = True
    
    pipe2.on("data", handler)
    await pipe1.write("test_chunk")
    assert called is True

def test_unsubscribe_handler(pipe:Pipe):
    # Positive case: Unsubscribing a handler
    called = False
    def handler(chunk: Any) -> None:
        nonlocal called
        called = True
    
    pipe.on("data", handler)
    pipe.unsubscribe("data", handler)
    asyncio.run(pipe._emit("data", "test_chunk"))
    assert called is False

def test_replace_on_handler(pipe):
    # Positive case: Replacing event handlers
    first_called = False
    second_called = False
    
    def first_handler(chunk: Any) -> None:
        nonlocal first_called
        first_called = True
    
    def second_handler(chunk: Any) -> None:
        nonlocal second_called
        second_called = True
    
    pipe.on("data", first_handler)
    pipe.replace_on_handler("data", second_handler)
    asyncio.run(pipe._emit("data", "test_chunk"))
    assert first_called is False
    assert second_called is True

@pytest.mark.asyncio
async def test_empty_event_emission(pipe):
    # Negative case: Emitting to non-existent event
    await pipe._emit("non_existent", "test_chunk")
    # Should not raise an exception

@pytest.mark.asyncio
async def test_handler_exception(pipe):
    # Negative case: Handler raises an exception
    def faulty_handler(chunk: Any) -> None:
        raise ValueError("Test error")
    
    pipe.on("data", faulty_handler)
    await pipe._emit("data", "test_chunk")
    # Exception should be caught and not propagate

@pytest.mark.asyncio
async def test_multiple_handlers(pipe):
    # Positive case: Multiple handlers for the same event
    call_count = 0
    def handler(chunk: Any) -> None:
        nonlocal call_count
        call_count += 1
    
    pipe.on("data", handler)
    pipe.on("data", handler)
    await pipe._emit("data", "test_chunk")
    assert call_count == 2

@pytest.mark.asyncio
async def test_pipeline_and_subscriber_interaction(pipe):
    # Positive case: Pipeline and subscriber interaction
    pipeline_called = False
    subscriber_called = False
    
    def pipeline_handler(chunk: Any) -> None:
        nonlocal pipeline_called
        pipeline_called = True
    
    def subscriber_handler(chunk: Any) -> None:
        nonlocal subscriber_called
        subscriber_called = True
    
    pipe.pipeline("data", pipeline_handler)
    pipe.on("data", subscriber_handler)
    await pipe._notify("data", "test_chunk")
    assert pipeline_called is True
    assert subscriber_called is True

def test_make_pipe_with_async_functions():
    # Positive case: Creating pipe with async writer and terminator
    async def async_writer(chunk: Any) -> None:
        pass
    
    async def async_terminator(*args: Any) -> None:
        pass
    
    pipe = Pipe.make_pipe(async_writer, async_terminator)
    assert isinstance(pipe, Pipe)

if __name__ == "__main__":
    pytest.main()
