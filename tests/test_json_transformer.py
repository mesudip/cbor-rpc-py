import pytest
import asyncio
import json
from typing import Any, Dict, List
from cbor_rpc import JsonTransformer
from cbor_rpc import Pipe

@pytest.mark.asyncio
async def test_json_transformer_basic_encoding_decoding():
    """Test basic JSON encoding and decoding."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)

    received_data = []
    transformer.on("data", lambda chunk: received_data.append(chunk))

    # Test encoding: write JSON data through pipe2 (will be received by transformer)
    test_data = {"message": "hello", "number": 42, "array": [1, 2, 3]}
    json_bytes = json.dumps(test_data).encode('utf-8')
    await pipe2.write(json_bytes)

    # Wait for data to be processed
    await asyncio.sleep(0.1)
    assert test_data == received_data[0] if received_data else None

    # Test decoding: write data through transformer (will be encoded)
    received_raw = []
    pipe2.on("data", lambda chunk: received_raw.append(chunk))
    await transformer.write({"response": "world", "success": True})

    # Read the encoded response from pipe2
    raw_response = received_raw[0] if received_raw else None
    decoded_response = json.loads(raw_response.decode('utf-8')) if raw_response else None
    assert decoded_response == {"response": "world", "success": True}

@pytest.mark.asyncio
async def test_json_transformer_create_pair():
    """Test creating a pair of JSON transformers."""
    transformer1, transformer2 = JsonTransformer.create_pair()

    # Test communication from transformer1 to transformer2
    received_data = []
    transformer2.on("data", lambda chunk: received_data.append(chunk))
    test_data = {"message": "Hello World", "timestamp": 1234567890}
    await transformer1.write(test_data)

    assert received_data == [test_data]

    # Test communication from transformer2 to transformer1
    received_data.clear()
    transformer1.on("data", lambda chunk: received_data.append(chunk))
    response_data = {"reply": "Hello Back", "status": "ok"}
    await transformer2.write(response_data)

    assert received_data == [response_data]

@pytest.mark.asyncio
async def test_json_transformer_different_data_types():
    """Test JSON transformer with different Python data types."""
    transformer1, transformer2 = JsonTransformer.create_pair()
    received_data = []
    transformer2.on("data", lambda chunk: received_data.append(chunk))

    # Test various data types
    test_cases = [
        "simple string",
        42,
        3.14159,
        True,
        False,
        None,
        [1, 2, 3, "mixed", True],
        {"nested": {"object": {"with": "values"}}},
        {"unicode": "Hello 世界 🌍"},
        [],
        {},
    ]

    for test_data in test_cases:
        await transformer1.write(test_data)

    assert received_data == test_cases

@pytest.mark.asyncio
async def test_json_transformer_encoding_errors():
    """Test JSON transformer encoding error handling."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)
    errors = []
    transformer.on("error", lambda err: errors.append(str(err)))

    # Test encoding non-serializable object
    class NonSerializable:
        pass

    result = await transformer.write(NonSerializable())
    assert result is False  # Write should return False on error
    assert len(errors) == 1

    # Test encoding circular reference
    circular = {}
    circular['self'] = circular
    result = await transformer.write(circular)
    assert result is False
    assert len(errors) == 2

@pytest.mark.asyncio
async def test_json_transformer_decoding_errors():
    """Test JSON transformer decoding error handling."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)
    errors = []
    transformer.on("error", lambda err: errors.append(str(err)))

    # Test invalid JSON
    await pipe2.write(b'{"invalid": json}')
    assert len(errors) == 1

    # Test invalid UTF-8 bytes
    await pipe2.write(b'\xff\xfe\xfd')
    assert len(errors) == 2

    # Test wrong data type
    await pipe2.write(123)  # Not bytes or string
    assert len(errors) == 3

@pytest.mark.asyncio
async def test_json_transformer_string_input():
    """Test JSON transformer with string input (not just bytes)."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)
    received_data = []
    transformer.on("data", lambda chunk: received_data.append(chunk))

    # Test with JSON string input
    test_data = {"message": "from string", "value": 123}
    json_string = json.dumps(test_data)
    await pipe2.write(json_string)

    assert received_data == [test_data]

@pytest.mark.asyncio
async def test_json_transformer_custom_encoding():
    """Test JSON transformer with custom text encoding."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1, encoding='latin1')
    received_data = []
    transformer.on("data", lambda chunk: received_data.append(chunk))

    # Test with latin1 encoding
    test_data = {"message": "café"}  # Contains non-ASCII character
    json_bytes = json.dumps(test_data).encode('latin1')
    await pipe2.write(json_bytes)

    assert received_data == [test_data]

@pytest.mark.asyncio
async def test_json_transformer_termination():
    """Test JSON transformer termination."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)
    close_events = []
    transformer.on("close", lambda *args: close_events.append(args))

    await transformer.terminate("test_reason")
    assert len(close_events) == 1
    assert close_events[0] == ("test_reason",)

@pytest.mark.asyncio
async def test_json_transformer_large_data():
    """Test JSON transformer with large data structures."""
    transformer1, transformer2 = JsonTransformer.create_pair()
    received_data = []
    transformer2.on("data", lambda chunk: received_data.append(chunk))

    # Create a large nested data structure
    large_data = {
        "users": [
            {
                "id": i,
                "name": f"User {i}",
                "email": f"user{i}@example.com",
                "metadata": {
                    "created": f"2024-01-{i:02d}",
                    "tags": [f"tag{j}" for j in range(5)],
                    "settings": {
                        "theme": "dark" if i % 2 else "light",
                        "notifications": True,
                        "features": [f"feature{k}" for k in range(3)]
                    }
                }
            }
            for i in range(10)
        ]
    }

    await transformer1.write(large_data)
    assert len(received_data) == 1
    assert received_data[0] == large_data

@pytest.mark.asyncio
async def test_json_transformer_concurrent_operations():
    """Test JSON transformer with concurrent read/write operations."""
    transformer1, transformer2 = JsonTransformer.create_pair()
    received_data = []
    transformer2.on("data", lambda chunk: received_data.append(chunk))

    # Send multiple messages concurrently
    async def send_message(id: int):
        await transformer1.write({"id": id, "message": f"Message {id}"})

    tasks = [send_message(i) for i in range(5)]
    await asyncio.gather(*tasks)

    assert len(received_data) == 5

@pytest.mark.asyncio
async def test_json_transformer_error_recovery():
    """Test that JSON transformer can recover from errors."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)
    received_data = []
    errors = []
    transformer.on("data", lambda chunk: received_data.append(chunk))
    transformer.on("error", lambda err: errors.append(str(err)))

    # Send invalid JSON
    await pipe2.write(b'invalid json')

    # Send valid JSON after error
    valid_data = {"message": "recovery test"}
    await pipe2.write(json.dumps(valid_data).encode('utf-8'))

    assert len(received_data) == 1
    assert received_data[0] == valid_data

if __name__ == "__main__":
    pytest.main(["-v", __file__])
