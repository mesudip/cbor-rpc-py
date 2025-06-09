import pytest
import asyncio
from typing import Any, Tuple
from cbor_rpc import EventPipe

class SimplePipe(EventPipe):
    def __init__(self):
        super().__init__()
        self._closed = False

    async def write(self, chunk: Any) -> bool:
        if self._closed:
            return False
        # Simulate writing data asynchronously
        await asyncio.sleep(0.1)
        return True

    async def terminate(self, *args: Any) -> None:
        if self._closed:
            return
        self._closed = True
        await self._emit("close", *args)

@pytest.fixture
def pipe():
    return SimplePipe()

@pytest.mark.asyncio
async def test_create_pair():
    # Positive case: Creating a pair of async pipes
    pipe1, pipe2 = EventPipe.create_pair()
    assert isinstance(pipe1, EventPipe)
    assert isinstance(pipe2, EventPipe)

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
async def test_pipeline_execution(pipe):
    # Positive case: Adding and executing a pipeline
    called = False
    async def pipeline_handler(chunk: Any) -> None:
        nonlocal called
        called = True

    pipe.pipeline("data", pipeline_handler)
    await pipe._notify("data", "test_chunk")
    assert called is True

@pytest.mark.asyncio
async def test_attach_pipes():
    # Positive case: Attaching two pipes
    pipe1, pipe2 = EventPipe.create_pair()

    called = False
    async def handler(chunk: Any) -> None:
        nonlocal called
        called = True

    pipe2.on("data", handler)
    await pipe1.write("test_chunk")
    assert called is True

@pytest.mark.asyncio
async def test_write_after_terminate():
    # Negative case: Writing to a terminated pipe
    pipe1, _ = EventPipe.create_pair()
    await pipe1.terminate()

    result = await pipe1.write("test_chunk")
    assert result is False

if __name__ == "__main__":
    pytest.main()
