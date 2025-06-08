import pytest
import asyncio
from typing import Any, Dict, List
from cbor_rpc.async_pipe import Pipe
from cbor_rpc import Transformer
from cbor_rpc import SyncPipe
from cbor_rpc import AbstractEmitter

# Existing tests...

@pytest.mark.asyncio
async def test_async_transformer_basic():
    """Test basic asynchronous transformer functionality."""
    pipe1, pipe2 = Pipe.create_pair()

    class MockTransformer(Transformer[str, str], AbstractEmitter):
        async def encode(self, data: str) -> str:
            return f"encoded_{data}"

        async def decode(self, data: Any) -> str:
            if isinstance(data, str) and data.startswith("encoded_"):
                return data[8:]  # Remove "encoded_" prefix
            raise ValueError("Invalid format")

    transformer = MockTransformer(pipe1)

    # Set up event handler on pipe2 to receive data
    received_data = None

    def handler(data: str) -> None:
        nonlocal received_data
        received_data = data

    pipe2.on("data", handler)

    # Write through transformer, should be received by pipe2
    assert await transformer.write("test_data") is True
    await asyncio.sleep(0.1)  # Give time for events to propagate
    assert received_data == "encoded_test_data"

@pytest.mark.asyncio
async def test_sync_transformer_basic():
    """Test basic synchronous transformer functionality."""
    pipe1, pipe2 = SyncPipe.create_pair()

    class MockSyncTransformer(Transformer[str, str]):
        def encode_sync(self, data: str) -> str:
            return f"encoded_{data}"

        def decode_sync(self, data: Any) -> str:
            if isinstance(data, str) and data.startswith("encoded_"):
                return data[8:]  # Remove "encoded_" prefix
            raise ValueError("Invalid format")

    transformer = MockSyncTransformer(pipe1)

    # Write through transformer, should be readable from pipe2
    assert transformer.write_sync("test_data") is True

    # Directly read from the connected sync pipe to verify data transfer
    encoded_data = pipe2.read(timeout=1.0)
    decoded_data = transformer.decode_sync(encoded_data) if encoded_data else None
    assert decoded_data == "test_data"

@pytest.mark.asyncio
async def test_transformer_close_propagation():
    """Test close propagation in transformers."""
    pipe1, pipe2 = Pipe.create_pair()

    class MockTransformer(Transformer[str, str]):
        async def encode(self, data: str) -> str:
            return f"encoded_{data}"

        async def decode(self, data: Any) -> str:
            if isinstance(data, str) and data.startswith("encoded_"):
                return data[8:]  # Remove "encoded_" prefix
            raise ValueError("Invalid format")

    transformer = MockTransformer(pipe1)

    # Close transformer and verify pipe is also closed
    await transformer.terminate()

    # Try to write after closing - should fail
    assert await transformer.write("test_data") is False

    # Verify pipe is closed by trying to write directly (should fail)
    assert await pipe1.write("raw_data") is False

@pytest.mark.asyncio
async def test_transformer_exception_handling():
    """Test exception handling in transformers."""
    pipe1, pipe2 = Pipe.create_pair()

    class FaultyTransformer(Transformer[str, str], AbstractEmitter):
        async def encode(self, data: str) -> str:
            raise ValueError("Encoding error")

        async def decode(self, data: Any) -> str:
            if isinstance(data, str) and data.startswith("encoded_"):
                return data[8:]  # Remove "encoded_" prefix
            raise ValueError("Invalid format")

    transformer = FaultyTransformer(pipe1)

    # Set up error handler to verify exception is caught on the transformer itself
    error_caught = False

    def on_error(err: Exception) -> None:
        nonlocal error_caught
        error_caught = True

    transformer.on("error", on_error)

    # Write through transformer - should trigger encoding error
    assert await transformer.write("test_data") is False
    assert error_caught is True

if __name__ == "__main__":
    pytest.main()
