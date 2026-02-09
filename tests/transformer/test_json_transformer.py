import asyncio
import json

import pytest

from cbor_rpc.pipe.aio_pipe import AioPipe
from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.tcp.tcp import TcpPipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.event_transformer_pipe import EventTransformerPipe
from cbor_rpc.transformer.json_transformer import JsonTransformer

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
    client_transformed_pipe = json_transformer.apply_transformer(client_raw_pipe)
    return client_raw_pipe, server_raw_pipe, client_transformed_pipe, json_transformer


@pytest.fixture
async def json_pipe_ascii(pipe_pair):
    client_raw_pipe, server_raw_pipe = pipe_pair
    json_transformer = JsonTransformer(encoding="ascii")
    client_transformed_pipe = json_transformer.apply_transformer(client_raw_pipe)
    return client_raw_pipe, server_raw_pipe, client_transformed_pipe, json_transformer


@pytest.mark.asyncio
class TestJsonTransformerPipeInteraction:
    async def test_json_transformer_end_to_end_simple_dict(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe
        assert isinstance(client_transformed_pipe, EventTransformerPipe)

        received_data_queue = asyncio.Queue()
        server_raw_pipe.on("data", received_data_queue.put_nowait)

        original_data = {"message": "Hello, world!", "number": 123}

        await client_transformed_pipe.write(original_data)

        encoded_data_received_by_server = await asyncio.wait_for(received_data_queue.get(), timeout=2.0)

        decoded_by_server = json.loads(encoded_data_received_by_server.decode("utf-8"))
        assert decoded_by_server == original_data

        client_received_data_queue = asyncio.Queue()
        client_transformed_pipe.on("data", client_received_data_queue.put_nowait)

        response_data = {"status": "success", "code": 200}
        await server_raw_pipe.write(json.dumps(response_data).encode("utf-8"))

        decoded_data_received_by_client = await asyncio.wait_for(
            client_received_data_queue.get(),
            timeout=2.0,
        )
        assert decoded_data_received_by_client == response_data

        await client_raw_pipe.terminate()
        await server_raw_pipe.terminate()

    async def test_json_transformer_end_to_end_unicode_characters(self, json_pipe):
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
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe_ascii

        original_data = {"message": "Hello, world! 👋"}

        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        with pytest.raises(UnicodeEncodeError):
            await client_transformed_pipe.write(original_data)

        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, UnicodeEncodeError)
        await asyncio.sleep(0)
        assert client_raw_pipe._closed is True

    async def test_json_transformer_decoding_error_on_read(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        invalid_json_bytes = b'{,"key": "value",}'
        try:
            await server_raw_pipe.write(invalid_json_bytes)
        except json.JSONDecodeError:
            pass

        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, json.JSONDecodeError)

    async def test_json_transformer_decoding_type_error_on_read(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        non_string_data = 12345
        try:
            await server_raw_pipe.write(non_string_data)
        except TypeError as exc:
            assert isinstance(exc, TypeError)
            return

        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, TypeError)
        assert "Expected bytes or str" in str(error)

    async def test_json_transformer_non_json_serializable_data(self, json_pipe):
        client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        non_serializable_data = {"set_data": {1, 2, 3}}

        error_queue = asyncio.Queue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        with pytest.raises(TypeError):
            await client_transformed_pipe.write(non_serializable_data)

        error = await asyncio.wait_for(error_queue.get(), timeout=DEFAULT_TIMEOUT)
        assert isinstance(error, TypeError)
        await asyncio.sleep(0)
        assert client_raw_pipe._closed is True

    async def test_json_transformer_pipe_termination(self, json_pipe):
        client_raw_pipe, _server_raw_pipe, client_transformed_pipe, _ = json_pipe

        close_event_received = asyncio.Event()
        client_transformed_pipe.on("close", lambda: close_event_received.set())

        await client_raw_pipe.terminate()

        await asyncio.wait_for(close_event_received.wait(), timeout=DEFAULT_TIMEOUT)

    async def test_json_transformer_pipe_write_after_termination(self, json_pipe):
        client_raw_pipe, _server_raw_pipe, client_transformed_pipe, _ = json_pipe

        await client_raw_pipe.terminate()

        if isinstance(client_raw_pipe, TcpPipe):
            with pytest.raises(ConnectionError):
                await client_transformed_pipe.write({"test": "data"})
        else:
            result = await client_transformed_pipe.write({"test": "data"})
            assert result is False

    async def test_json_transformer_pipe_read_after_termination(self, json_pipe):
        _client_raw_pipe, server_raw_pipe, client_transformed_pipe, _ = json_pipe

        data_queue = asyncio.Queue()
        client_transformed_pipe.pipeline("data", data_queue.put_nowait)

        close_event_received = asyncio.Event()
        client_transformed_pipe.on("close", lambda: close_event_received.set())

        await server_raw_pipe.terminate()
        await asyncio.wait_for(close_event_received.wait(), timeout=DEFAULT_TIMEOUT)

        try:
            result = await server_raw_pipe.write(b'{"should": "not_receive"}')
            assert result is False
        except ConnectionError:
            pass

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(data_queue.get(), timeout=0.1)


def test_json_transformer_decode_variants():
    transformer = JsonTransformer()
    assert transformer.decode(b'{"x": 1}') == {"x": 1}
    assert transformer.decode('{"y": 2}') == {"y": 2}

    with pytest.raises(TypeError):
        transformer.decode(123)
