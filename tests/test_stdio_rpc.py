import asyncio
import pytest
import sys
from cbor_rpc.stdio.stdio_pipe import StdioPipe


@pytest.mark.asyncio
async def test_stdtio_read_write():
    """
    Tests the StdioPipe.start_process method by writing and reading data 10 times.
    """
    pipe = await StdioPipe.start_process("/bin/bash", "-c", "cat -")

    # List to collect received data
    received_data = []
    future = asyncio.Future()

    def on_data(data):
        received_data.append(data)
        # Complete the future after receiving 10 data events
        if len(received_data) == 10:
            future.set_result(None)

    pipe.pipeline("data", on_data)

    # Write 10 unique data chunks
    test_data = [f"Test data {i}\n".encode("utf-8") for i in range(10)]
    for data in test_data:
        await pipe.write(data)
        # Brief sleep to allow the subprocess to process the data
        await asyncio.sleep(0.01)

    # Wait for all 10 data events
    await future

    # Assert that all received data matches the sent data
    assert len(received_data) == 10, f"Expected 10 data events, got {len(received_data)}"
    for i, (sent, received) in enumerate(zip(test_data, received_data)):
        assert received == sent, f"Mismatch at index {i}: expected {sent!r}, got {received!r}"

    # Terminate the pipe
    pipe.terminate()
