import pytest
import json
import asyncio
from cbor_rpc.transformer.json_transformer import JsonTransformer
from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.event_transformer_pipe import EventTransformerPipe

@pytest.mark.asyncio
class TestJsonTransformerPipeInteraction:

    async def test_json_transformer_end_to_end_simple_dict(self):
        # Create a pair of event pipes
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()

        # Instantiate the JSON transformer
        json_transformer = JsonTransformer()

        # Apply the transformer to the client side of the raw pipe
        # This creates an EventTransformerPipe that encodes/decodes data
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)
        assert isinstance(client_transformed_pipe, EventTransformerPipe)

        # Use a queue to capture data emitted by the server_raw_pipe
        received_data_queue = asyncio.Queue()
        server_raw_pipe.on("data", received_data_queue.put_nowait)

        # Data to send
        original_data = {"message": "Hello, world!", "number": 123}

        # Write original_data to the transformed client pipe
        # This should encode the data and send it through client_raw_pipe to server_raw_pipe
        await client_transformed_pipe.write(original_data)

        # Wait for the encoded data to arrive at the server_raw_pipe
        # The server_raw_pipe receives the *encoded* data (bytes)
        encoded_data_received_by_server = await received_data_queue.get()

        # Manually decode the data received by the server_raw_pipe to verify it's JSON bytes
        decoded_by_server = json.loads(encoded_data_received_by_server.decode('utf-8'))
        assert decoded_by_server == original_data

        # Now, let's test the reverse: server sends data, client receives decoded data
        # Use a queue to capture data emitted by the client_transformed_pipe
        client_received_data_queue = asyncio.Queue()
        client_transformed_pipe.on("data", client_received_data_queue.put_nowait)

        # Data to send from server
        response_data = {"status": "success", "code": 200}
        
        # Server_raw_pipe writes the *encoded* data (as if it received it from a client and is sending a response)
        # This data will go through client_raw_pipe and then be decoded by client_transformed_pipe
        await server_raw_pipe.write(json.dumps(response_data).encode('utf-8'))

        # Wait for the decoded data to arrive at the client_transformed_pipe
        decoded_data_received_by_client = await client_received_data_queue.get()
        assert decoded_data_received_by_client == response_data

        # Clean up
        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_end_to_end_unicode_characters(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        json_transformer = JsonTransformer()
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        received_data_queue = asyncio.Queue()
        server_raw_pipe.on("data", received_data_queue.put_nowait)

        original_data = {"message": "你好世界 👋"}
        await client_transformed_pipe.write(original_data)
        encoded_data_received_by_server = await received_data_queue.get()
        decoded_by_server = json.loads(encoded_data_received_by_server.decode('utf-8'))
        assert decoded_by_server == original_data

        client_received_data_queue = asyncio.Queue()
        client_transformed_pipe.on("data", client_received_data_queue.put_nowait)
        response_data = {"greeting": "こんにちは"}
        await server_raw_pipe.write(json.dumps(response_data, ensure_ascii=False).encode('utf-8'))
        decoded_data_received_by_client = await client_received_data_queue.get()
        assert decoded_data_received_by_client == response_data

        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_encoding_error_on_write(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        # Use an encoding that cannot handle certain characters
        json_transformer = JsonTransformer(encoding='ascii')
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        original_data = {"message": "Hello, world! 👋"} # Contains non-ASCII character

        # Expect an encoding error when writing
        with pytest.raises(UnicodeEncodeError):
            await client_transformed_pipe.write(original_data)
        
        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_decoding_error_on_read(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        json_transformer = JsonTransformer()
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        # Use a queue to capture errors emitted by the transformed pipe
        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        # Simulate server sending invalid JSON bytes
        invalid_json_bytes = b'{"key": "value",}' # Invalid JSON
        await server_raw_pipe.write(invalid_json_bytes)

        # The transformed pipe should emit an error when trying to decode
        error = await asyncio.wait_for(error_queue.get(), timeout=1)
        assert isinstance(error, json.JSONDecodeError)

        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_decoding_type_error_on_read(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        json_transformer = JsonTransformer()
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        # Simulate server sending non-bytes/str data (e.g., an int)
        non_string_data = 12345
        await server_raw_pipe.write(non_string_data) # This will pass through raw pipe as is

        # The transformed pipe should emit a TypeError when trying to decode
        error = await asyncio.wait_for(error_queue.get(), timeout=1)
        assert isinstance(error, TypeError)
        assert "Expected bytes or str" in str(error)

        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_non_json_serializable_data(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        json_transformer = JsonTransformer()
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        # Data that is not JSON serializable
        non_serializable_data = {"set_data": {1, 2, 3}}

        # Writing this data should raise a TypeError
        with pytest.raises(TypeError):
            await client_transformed_pipe.write(non_serializable_data)
        
        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_pipe_termination(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        json_transformer = JsonTransformer()
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        # Listen for close event on the transformed pipe
        close_event_received = asyncio.Event()
        client_transformed_pipe.on("close", lambda: close_event_received.set())

        # Terminate the underlying raw pipe
        await client_raw_pipe.terminate()

        # The transformed pipe should also terminate and emit a close event
        await asyncio.wait_for(close_event_received.wait(), timeout=1)
        await server_raw_pipe.terminate() # Ensure the other end is also terminated

    async def test_json_transformer_pipe_write_after_termination(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        json_transformer = JsonTransformer()
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        await client_raw_pipe.terminate()
        
        # Writing to a terminated transformed pipe should return False
        result = await client_transformed_pipe.write({"test": "data"})
        assert result is False

        await server_raw_pipe.terminate()

    async def test_json_transformer_pipe_read_after_termination(self):
        client_raw_pipe, server_raw_pipe = EventPipe.create_pair()
        json_transformer = JsonTransformer()
        client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)

        # Listen for data on the transformed pipe
        data_queue = asyncio.Queue()
        client_transformed_pipe.on("data", data_queue.put_nowait)

        # Terminate the server_raw_pipe, which should cause the client_transformed_pipe to terminate
        await server_raw_pipe.terminate()

        # The transformed pipe should eventually close and not emit new data
        close_event_received = asyncio.Event()
        client_transformed_pipe.on("close", lambda: close_event_received.set())
        await asyncio.wait_for(close_event_received.wait(), timeout=1)

        # Try to write to the raw pipe from the server side after termination
        # This data should not be processed by the transformed pipe
        await server_raw_pipe.write(b'{"should": "not_receive"}')
        
        # Ensure no data is received by the transformed pipe after termination
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(data_queue.get(), timeout=0.1)

        await client_raw_pipe.terminate()
