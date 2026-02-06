import pytest
import json
import asyncio
from cbor_rpc.tcp.tcp import TcpPipe
from cbor_rpc.transformer.json_transformer import JsonTransformer
from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.pipe.aio_pipe import AioPipe
from tests.helpers.simple_pipe import SimplePipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.event_transformer_pipe import EventTransformerPipe

DEFAULT_TIMEOUT = 2.0


@pytest.fixture(
    params=[
        (EventPipe.create_inmemory_pair, "InmemoryPipe"),
        (AioPipe.create_inmemory_pair, "AioPipe"),
        (TcpPipe.create_inmemory_pair, "TcpPipe"),
    ],
    ids=lambda param: param[1],
)
async def pipe_pair(request):
    create_pair_func, _ = request.param
    if asyncio.iscoroutinefunction(create_pair_func):
        client_pipe, server_pipe = await create_pair_func()
    else:
        client_pipe, server_pipe = create_pair_func()
    yield client_pipe, server_pipe
    await client_pipe.terminate()
    await server_pipe.terminate()


@pytest.fixture
async def json_pipe(pipe_pair):
    client_raw_pipe, server_raw_pipe = pipe_pair
    json_transformer = JsonTransformer()
    client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)
    return client_raw_pipe, server_raw_pipe, client_transformed_pipe, json_transformer


@pytest.fixture
async def json_pipe_ascii(pipe_pair):
    client_raw_pipe, server_raw_pipe = pipe_pair
    json_transformer = JsonTransformer(encoding="ascii")
    client_transformed_pipe = json_transformer.applyTransformer(client_raw_pipe)
    return client_raw_pipe, server_raw_pipe, client_transformed_pipe, json_transformer


@pytest.mark.asyncio
class TestJsonTransformerPipeInteraction:

    async def test_json_transformer_end_to_end_simple_dict(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe
        assert isinstance(client_transformed_pipe, EventTransformerPipe)

        # Use a queue to capture data emitted by the server_raw_pipe
        received_data_queue = asyncio.Queue()
        server_raw_pipe.on("error", lambda e: print("Server raw pipe error:", e))

        server_raw_pipe.on("data", received_data_queue.put_nowait)

        # Data to send
        original_data = {"message": "Hello, world!", "number": 123}

        # Write original_data to the transformed client pipe
        # This should encode the data and send it through client_raw_pipe to server_raw_pipe
        await client_transformed_pipe.write(original_data)

        # Wait for the encoded data to arrive at the server_raw_pipe
        # The server_raw_pipe receives the *encoded* data (bytes)
        try:
            # Wait for data for up to 5 seconds
            encoded_data_received_by_server = await asyncio.wait_for(received_data_queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            print("No data received within 5 seconds")
            assert False, "Test failed due to timeout waiting for data"

        # Manually decode the data received by the server_raw_pipe to verify it's JSON bytes
        decoded_by_server = json.loads(encoded_data_received_by_server.decode("utf-8"))
        assert decoded_by_server == original_data

        # Now, let's test the reverse: server sends data, client receives decoded data
        # Use a queue to capture data emitted by the client_transformed_pipe
        client_received_data_queue = asyncio.Queue()
        client_transformed_pipe.on("data", client_received_data_queue.put_nowait)

        # Data to send from server
        response_data = {"status": "success", "code": 200}

        # Server_raw_pipe writes the *encoded* data (as if it received it from a client and is sending a response)
        # This data will go through client_raw_pipe and then be decoded by client_transformed_pipe
        await server_raw_pipe.write(json.dumps(response_data).encode("utf-8"))

        # Wait for the decoded data to arrive at the client_transformed_pipe
        decoded_data_received_by_client = await asyncio.wait_for(
            client_received_data_queue.get(),
            timeout=2.0,
        )
        assert decoded_data_received_by_client == response_data

        # Clean up
        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_end_to_end_unicode_characters(self, json_pipe):
        ## TODO: This is taking forever when using TCP pipes, investigate why. It works fine with EventPipe and AioPipe.
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        received_data_queue = asyncio.Queue()
        server_raw_pipe.on("data", received_data_queue.put_nowait)

        original_data = {"message": "你好世界 👋"}
        await client_transformed_pipe.write(original_data)
        encoded_data_received_by_server = await asyncio.wait_for(
            received_data_queue.get(),
            timeout=2.0,
        )
        decoded_by_server = json.loads(encoded_data_received_by_server.decode("utf-8"))
        assert decoded_by_server == original_data

        client_received_data_queue = asyncio.Queue()
        client_transformed_pipe.on("data", client_received_data_queue.put_nowait)
        response_data = {"greeting": "こんにちは"}
        await server_raw_pipe.write(json.dumps(response_data, ensure_ascii=False).encode("utf-8"))
        decoded_data_received_by_client = await asyncio.wait_for(
            client_received_data_queue.get(),
            timeout=2.0,
        )
        assert decoded_data_received_by_client == response_data

    async def test_json_transformer_encoding_error_on_write(self, json_pipe_ascii):
        # Use an encoding that cannot handle certain characters
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe_ascii

        original_data = {"message": "Hello, world! 👋"}  # Contains non-ASCII character

        # Use a queue to capture errors emitted by the transformed pipe
        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        # Writing this data should cause an encoding error to be emitted
        await client_transformed_pipe.write(original_data)

        # Assert that a UnicodeEncodeError is received
        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, UnicodeEncodeError)

    async def test_json_transformer_decoding_error_on_read(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        # Use a queue to capture errors emitted by the transformed pipe
        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        # Simulate server sending invalid JSON bytes
        invalid_json_bytes = b'{,"key": "value",}'  # Invalid JSON
        try:
            await server_raw_pipe.write(invalid_json_bytes)
        except json.JSONDecodeError:
            # EventPipe/AioPipe may raise from the pipeline while still emitting the error.
            pass

        # The transformed pipe should emit an error when trying to decode
        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, json.JSONDecodeError)

    async def test_json_transformer_decoding_type_error_on_read(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        # Simulate server sending non-bytes/str data (e.g., an int)
        non_string_data = 12345
        try:
            await server_raw_pipe.write(non_string_data)  # This will pass through raw pipe as is
        except TypeError as exc:
            # TcpPipe enforces bytes-only writes; no error will be emitted by the transformer.
            assert isinstance(exc, TypeError)
            return

        # The transformed pipe should emit a TypeError when trying to decode
        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, TypeError)
        assert "Expected bytes or str" in str(error)

    async def test_json_transformer_non_json_serializable_data(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        # Data that is not JSON serializable
        non_serializable_data = {"set_data": {1, 2, 3}}

        # Use a queue to capture errors emitted by the transformed pipe
        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        # Writing this data should cause a TypeError to be emitted
        await client_transformed_pipe.write(non_serializable_data)

        # Assert that a TypeError is received
        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, TypeError)

    async def test_json_transformer_pipe_termination(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        # Listen for close event on the transformed pipe
        close_event_received = asyncio.Event()
        client_transformed_pipe.on("close", lambda: close_event_received.set())

        # Terminate the underlying raw pipe
        await client_raw_pipe.terminate()

        # The transformed pipe should also terminate and emit a close event
        await asyncio.wait_for(close_event_received.wait(), timeout=DEFAULT_TIMEOUT)
        # server_raw_pipe is terminated by the fixture

    async def test_json_transformer_pipe_write_after_termination(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        await client_raw_pipe.terminate()

        # Writing to a terminated transformed pipe should return False
        result = await client_transformed_pipe.write({"test": "data"})
        assert result is False

        # server_raw_pipe is terminated by the fixture

    async def test_json_transformer_pipe_read_after_termination(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        # Listen for data on the transformed pipe
        data_queue = asyncio.Queue()
        client_transformed_pipe.pipeline("data", data_queue.put_nowait)

        # Terminate the server_raw_pipe, which should cause the client_transformed_pipe to terminate

        # The transformed pipe should eventually close and not emit new data
        close_event_received = asyncio.Event()
        client_transformed_pipe.on("close", lambda: close_event_received.set())

        await server_raw_pipe.terminate()
        await asyncio.wait_for(close_event_received.wait(), timeout=DEFAULT_TIMEOUT)

        # Try to write to the raw pipe from the server side after termination
        # This data should not be processed by the transformed pipe
        try:
            result = await server_raw_pipe.write(b'{"should": "not_receive"}')
            assert result is False
        except ConnectionError:
            # TcpPipe raises if not connected after termination.
            pass

        # Ensure no data is received by the transformed pipe after termination
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(data_queue.get(), timeout=0.1)

        # client_raw_pipe is terminated by the fixture
