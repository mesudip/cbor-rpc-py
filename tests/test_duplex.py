import asyncio
import pytest
from typing import Any, Generic, TypeVar
from cbor_rpc.rpc import Duplex, SimplePipe, Pipe

T1 = TypeVar('T1')
T2 = TypeVar('T2')

@pytest.mark.asyncio
async def test_parallel_read_write():
    """Test parallel reading and writing to Duplex."""
    duplex = Duplex[str, str]()
    received_data = []

    # Set up reader handler
    async def reader_handler(data: str):
        received_data.append(data)

    duplex.on("data", reader_handler)

    # Write multiple messages in parallel
    write_tasks = [
        duplex.write(f"message_{i}")
        for i in range(5)
    ]
    await asyncio.gather(*write_tasks)

    # Allow time for processing
    await asyncio.sleep(0.1)

    assert len(received_data) == 5
    assert all(f"message_{i}" in received_data for i in range(5))

@pytest.mark.asyncio
async def test_multiple_readers():
    """Test multiple readers receiving same data."""
    duplex = Duplex[str, str]()
    reader1_data = []
    reader2_data = []

    duplex.on("data", lambda x: reader1_data.append(x))
    duplex.on("data", lambda x: reader2_data.append(x))

    await duplex.write("test_message")
    await asyncio.sleep(0.1)

    assert reader1_data == ["test_message"]
    assert reader2_data == ["test_message"]

@pytest.mark.asyncio
async def test_termination():
    """Test proper termination handling."""
    duplex = Duplex[str, str]()
    close_events = []

    duplex.on("close", lambda *args: close_events.append(args))

    await duplex.terminate("test_reason")
    await asyncio.sleep(0.1)

    assert len(close_events) == 1
    assert close_events[0] == ("test_reason",)

@pytest.mark.asyncio
async def test_write_after_termination():
    """Test writing after termination (negative case)."""
    duplex = Duplex[str, str]()
    await duplex.terminate()

    # Writing after termination should still be possible but have no effect
    await duplex.write("should_be_ignored")
    received_data = []
    duplex.on("data", lambda x: received_data.append(x))
    await asyncio.sleep(0.1)

    assert received_data == []

@pytest.mark.asyncio
async def test_error_propagation():
    """Test error propagation from reader/writer."""
    duplex = Duplex[str, str]()
    errors = []

    duplex.on("error", lambda err: errors.append(str(err)))

    # Simulate error in reader
    async def error_handler(data: str):
        raise ValueError("Reader error")
    
    duplex.reader.on("data", error_handler)
    await duplex.reader.write("test")
    await asyncio.sleep(0.1)

    assert len(errors) == 1
    assert "Reader error" in errors[0]

@pytest.mark.asyncio
async def test_pipeline_failure():
    """Test pipeline failure handling."""
    duplex = Duplex[str, str]()
    errors = []

    duplex.on("error", lambda err: errors.append(str(err)))

    # Simulate pipeline failure
    async def failing_pipeline(data: str):
        raise RuntimeError("Pipeline failure")
    
    duplex.pipeline("data", failing_pipeline)
    await duplex.write("test")
    await asyncio.sleep(0.1)

    assert len(errors) == 1
    assert "Pipeline failure" in errors[0]

@pytest.mark.asyncio
async def test_invalid_data_type():
    """Test handling of invalid data types (negative case)."""
    duplex = Duplex[str, int]()  # Expecting str input, int output
    received_data = []

    duplex.on("data", lambda x: received_data.append(x))

    # Write invalid type
    await duplex.write(123)  # Should still process as Duplex is type-agnostic at runtime
    await asyncio.sleep(0.1)

    assert received_data == [123]  # Type checking is not enforced at runtime

@pytest.mark.asyncio
async def test_concurrent_read_write_stress():
    """Stress test with concurrent reads and writes."""
    duplex = Duplex[str, str]()
    received_data = []
    duplex.on("data", lambda x: received_data.append(x))

    async def writer(id: int):
        for i in range(100):
            await duplex.write(f"msg_{id}_{i}")

    # Start multiple writers
    writers = [writer(i) for i in range(5)]
    await asyncio.gather(*writers)
    await asyncio.sleep(0.5)

    assert len(received_data) == 500
    assert all(any(f"msg_{j}_{i}" in received_data for j in range(5)) for i in range(100))

@pytest.mark.asyncio
async def test_empty_write():
    """Test writing empty data."""
    duplex = Duplex[str, str]()
    received_data = []

    duplex.on("data", lambda x: received_data.append(x))
    await duplex.write("")
    await asyncio.sleep(0.1)

    assert received_data == [""]

@pytest.mark.asyncio
async def test_reader_unsubscribe():
    """Test unsubscribing a reader."""
    duplex = Duplex[str, str]()
    received_data = []

    def reader_handler(data: str):
        received_data.append(data)

    duplex.on("data", reader_handler)
    await duplex.write("first")
    duplex.unsubscribe("data", reader_handler)
    await duplex.write("second")
    await asyncio.sleep(0.1)

    assert received_data == ["first"]
