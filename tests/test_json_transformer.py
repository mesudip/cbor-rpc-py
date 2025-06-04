import pytest
import asyncio
import json
from typing import Any, Dict, List
from cbor_rpc import Pipe, JsonTransformer


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
    await asyncio.sleep(0.01)
    
    assert received_data == [test_data]
    
    # Test decoding: write data through transformer (will be encoded)
    received_raw = []
    pipe2.on("data", lambda chunk: received_raw.append(chunk))
    
    await transformer.write({"response": "world", "success": True})
    await asyncio.sleep(0.01)
    
    assert len(received_raw) == 1
    decoded = json.loads(received_raw[0].decode('utf-8'))
    assert decoded == {"response": "world", "success": True}


@pytest.mark.asyncio
async def test_json_transformer_create_pair():
    """Test creating a pair of JSON transformers."""
    transformer1, transformer2 = JsonTransformer.create_pair()
    
    # Test communication from transformer1 to transformer2
    received_data = []
    transformer2.on("data", lambda chunk: received_data.append(chunk))
    
    test_data = {"message": "Hello World", "timestamp": 1234567890}
    await transformer1.write(test_data)
    await asyncio.sleep(0.01)
    
    assert received_data == [test_data]
    
    # Test communication from transformer2 to transformer1
    received_data.clear()
    transformer1.on("data", lambda chunk: received_data.append(chunk))
    
    response_data = {"reply": "Hello Back", "status": "ok"}
    await transformer2.write(response_data)
    await asyncio.sleep(0.01)
    
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
        await asyncio.sleep(0.01)
    
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
    await asyncio.sleep(0.01)
    
    assert len(errors) == 1
    assert "Object is not JSON serializable" in errors[0]
    
    # Test encoding circular reference
    circular = {}
    circular['self'] = circular
    
    result = await transformer.write(circular)
    assert result is False
    await asyncio.sleep(0.01)
    
    assert len(errors) == 2
    assert "Object is not JSON serializable" in errors[1]


@pytest.mark.asyncio
async def test_json_transformer_decoding_errors():
    """Test JSON transformer decoding error handling."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)
    
    errors = []
    transformer.on("error", lambda err: errors.append(str(err)))
    
    # Test invalid JSON
    await pipe2.write(b'{"invalid": json}')
    await asyncio.sleep(0.01)
    
    assert len(errors) == 1
    assert "Invalid JSON" in errors[0]
    
    # Test invalid UTF-8 bytes
    await pipe2.write(b'\xff\xfe\xfd')
    await asyncio.sleep(0.01)
    
    assert len(errors) == 2
    assert "Failed to decode bytes" in errors[1]
    
    # Test wrong data type
    await pipe2.write(123)  # Not bytes or string
    await asyncio.sleep(0.01)
    
    assert len(errors) == 3
    assert "Expected bytes or str" in errors[2]


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
    await asyncio.sleep(0.01)
    
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
    await asyncio.sleep(0.01)
    
    assert received_data == [test_data]
    
    # Test encoding with custom encoding
    received_raw = []
    pipe2.on("data", lambda chunk: received_raw.append(chunk))
    
    await transformer.write({"response": "café"})
    await asyncio.sleep(0.01)
    
    assert len(received_raw) == 1
    # Verify it was encoded with latin1
    decoded = json.loads(received_raw[0].decode('latin1'))
    assert decoded == {"response": "café"}


@pytest.mark.asyncio
async def test_json_transformer_termination():
    """Test JSON transformer termination."""
    pipe1, pipe2 = Pipe.create_pair()
    transformer = JsonTransformer(pipe1)
    
    close_events = []
    transformer.on("close", lambda *args: close_events.append(args))
    
    await transformer.terminate("test_reason")
    await asyncio.sleep(0.01)
    
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
            for i in range(100)
        ]
    }
    
    await transformer1.write(large_data)
    await asyncio.sleep(0.1)  # Give more time for large data
    
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
    
    tasks = [send_message(i) for i in range(10)]
    await asyncio.gather(*tasks)
    await asyncio.sleep(0.1)
    
    assert len(received_data) == 10
    
    # Verify all messages were received (order may vary due to concurrency)
    received_ids = {msg["id"] for msg in received_data}
    expected_ids = set(range(10))
    assert received_ids == expected_ids


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
    await asyncio.sleep(0.01)
    
    # Send valid JSON after error
    valid_data = {"message": "recovery test"}
    await pipe2.write(json.dumps(valid_data).encode('utf-8'))
    await asyncio.sleep(0.01)
    
    # Should have one error and one successful decode
    assert len(errors) == 1
    assert len(received_data) == 1
    assert received_data[0] == valid_data


if __name__ == "__main__":
    pytest.main(["-v", __file__])
