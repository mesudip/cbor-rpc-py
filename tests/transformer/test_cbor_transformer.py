import asyncio

import cbor2
import pytest
import pytest_asyncio

from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.tcp.tcp import TcpPipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.event_transformer_pipe import EventTransformerPipe
from cbor_rpc.transformer.cbor_transformer import CborTransformer, CborStreamTransformer
from tests.helpers.timeout_queue import TimeoutQueue


@pytest.fixture(
    params=[
        (EventPipe.create_inmemory_pair, "InmemoryPipe"),
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
def client_raw(pipe_pair):
    client_raw_pipe, _ = pipe_pair
    return client_raw_pipe


@pytest.fixture
def server_raw(pipe_pair):
    _, server_raw_pipe = pipe_pair
    return server_raw_pipe


@pytest.fixture
def client_cbor(client_raw):
    cbor_transformer = CborTransformer()
    return cbor_transformer.apply_transformer(client_raw)


@pytest.mark.asyncio
class TestCborTransformer:
    async def test_cbor_transformer_end_to_end_simple_dict(self, client_raw, server_raw, client_cbor):
        client_transformed_pipe = client_cbor

        received_data_queue = TimeoutQueue()
        server_raw.on("data", received_data_queue.put_nowait)

        original_data = {"message": "Hello, CBOR!", "number": 456, "list": [1, 2, 3]}
        await client_transformed_pipe.write(original_data)
        encoded_data_received_by_server = await received_data_queue.get()

        decoded_by_server = cbor2.loads(encoded_data_received_by_server)
        assert decoded_by_server == original_data

        client_received_data_queue = TimeoutQueue()
        client_transformed_pipe.on("data", client_received_data_queue.put_nowait)

        response_data = {"status": "cbor_success", "code": 200}
        await server_raw.write(cbor2.dumps(response_data))
        decoded_data_received_by_client = await client_received_data_queue.get()
        assert decoded_data_received_by_client == response_data

    async def test_cbor_transformer_decoding_error_on_read(self, server_raw, client_cbor,client_raw):
        client_transformed_pipe = client_cbor

        error_queue = TimeoutQueue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        incomplete_cbor_bytes = b"\x83\x01\x02"
        assert await server_raw.write(incomplete_cbor_bytes)

        error = await asyncio.wait_for(error_queue.get(), timeout=1)
        assert type(error).__name__.startswith("CBORDecode")
        # sleep 2
        await asyncio.sleep(0.2)
        assert server_raw._closed is True, "Pipe should  be closed on decode error"
        assert server_raw._closed is True, "Pipe should  be closed on decode error"

    async def test_cbor_transformer_non_bytes_data(self, server_raw, client_cbor):
        transformer = CborTransformer()
        with pytest.raises(TypeError):
            transformer.decode("not cbor")

    async def test_cbor_transformer_none_data(self, server_raw, client_cbor):
        transformer = CborTransformer()
        with pytest.raises(TypeError):
            transformer.decode(None)

    async def test_cbor_transformer_multiple_separate_writes(self, server_raw, client_cbor, client_raw):
        client_transformed_pipe = client_cbor

        received_data_queue = TimeoutQueue()
        received_errors = []
        client_transformed_pipe.pipeline("data", received_data_queue.put_nowait)
        client_transformed_pipe.on("error", lambda e: received_errors.append(e))

        assert await server_raw.write(cbor2.dumps({"a": 1}))
        decoded1 = await received_data_queue.get()
        await asyncio.sleep(0.05)
        assert await server_raw.write(cbor2.dumps({"b": 2}))

        assert [] == received_errors, f"Expected no errors, got: {received_errors}"
        decoded2 = await received_data_queue.get()

        assert decoded1 == {"a": 1}
        assert decoded2 == {"b": 2}

    async def test_cbor_transformer_single_concatenated_write(self, server_raw, client_cbor):
        client_transformed_pipe = client_cbor

        received_data_queue = TimeoutQueue()
        received_errors = []
        client_transformed_pipe.pipeline("data", received_data_queue.put_nowait)
        client_transformed_pipe.on("error", lambda e: received_errors.append(e))

        concatenated = cbor2.dumps({"a": 1}) + cbor2.dumps({"b": 2})
        assert await server_raw.write(concatenated)

        decoded1 = await received_data_queue.get()
        assert decoded1 == {"a": 1}
        assert [] == received_errors, f"Expected no errors, got: {received_errors}"

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(received_data_queue.get(), timeout=0.1)

    async def test_cbor_transformer_encode_error_on_write(self, server_raw, client_cbor):
        client_transformed_pipe = client_cbor

        error_queue = TimeoutQueue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        received_data_queue = TimeoutQueue()
        server_raw.on("data", received_data_queue.put_nowait)

        unserializable = {"func": lambda x: x}
        result = await client_transformed_pipe.write(unserializable)

        assert result is False
        error = await asyncio.wait_for(error_queue.get(), timeout=1)
        assert isinstance(error, Exception)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(received_data_queue.get(), timeout=0.1)

    async def test_cbor_transformer_close_propagation_and_write_after_close(self, client_raw, client_cbor):
        client_transformed_pipe = client_cbor

        close_queue = TimeoutQueue()
        client_transformed_pipe.on("close", lambda *args: close_queue.put_nowait(True))

        await client_raw.terminate()

        await close_queue.get()

        result = await client_transformed_pipe.write({"after": "close"})
        assert result is False


@pytest.mark.asyncio
class TestCborStreamTransformer:
    async def test_cbor_stream_transformer_single_object(self, client_raw, server_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        received_data_queue = TimeoutQueue()
        client_transformed_pipe.on("data", received_data_queue.put_nowait)

        original_data = {"key": "value", "id": 1}
        await server_raw.write(cbor2.dumps(original_data))

        decoded_data = await received_data_queue.get()
        assert decoded_data == original_data

    async def test_cbor_stream_transformer_concatenated_objects(self, client_raw, server_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        received_data_queue = TimeoutQueue()
        client_transformed_pipe.on("data", received_data_queue.put_nowait)

        obj1 = {"a": 1}
        obj2 = {"b": 2, "c": [3, 4]}
        obj3 = "hello"

        concatenated_cbor = cbor2.dumps(obj1) + cbor2.dumps(obj2) + cbor2.dumps(obj3)

        await server_raw.write(concatenated_cbor)

        await server_raw.write(b"")
        await server_raw.write(b"")

        decoded1 = await received_data_queue.get()
        decoded2 = await received_data_queue.get()
        decoded3 = await received_data_queue.get()

        assert decoded1 == obj1
        assert decoded2 == obj2
        assert decoded3 == obj3

    async def test_cbor_stream_transformer_single_concatenated_write(self, client_raw, server_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        received_data_queue = TimeoutQueue()
        received_errors = []
        client_transformed_pipe.on("data", received_data_queue.put_nowait)
        client_transformed_pipe.on("error", lambda e: received_errors.append(e))

        concatenated = cbor2.dumps({"a": 1}) + cbor2.dumps({"b": 2})
        await server_raw.write(concatenated)

        decoded1 = await received_data_queue.get()
        decoded2 = await received_data_queue.get()
        assert decoded1 == {"a": 1}
        assert decoded2 == {"b": 2}
        assert [] == received_errors, f"Expected no errors, got: {received_errors}"

    async def test_cbor_stream_transformer_delayed_separate_writes(self, client_raw, server_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        received_data_queue = TimeoutQueue()
        received_errors = []
        client_transformed_pipe.on("data", received_data_queue.put_nowait)
        client_transformed_pipe.on("error", lambda e: received_errors.append(e))

        assert await server_raw.write(cbor2.dumps({"a": 1}))
        decoded1 = await received_data_queue.get()
        await asyncio.sleep(0.05)
        assert await server_raw.write(cbor2.dumps({"b": 2}))

        decoded2 = await received_data_queue.get()
        assert decoded1 == {"a": 1}
        assert decoded2 == {"b": 2}
        assert [] == received_errors, f"Expected no errors, got: {received_errors}"

    async def test_cbor_stream_transformer_fragmented_objects(self, client_raw, server_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        received_data_queue = TimeoutQueue()
        client_transformed_pipe.on("data", received_data_queue.put_nowait)
        error_queue = TimeoutQueue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        obj = {"long_message": "a" * 100}
        cbor_bytes = cbor2.dumps(obj)

        await server_raw.write(cbor_bytes[:10])
        await asyncio.sleep(0.05)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(received_data_queue.get(), timeout=0.1)
        assert error_queue.empty()

        await server_raw.write(cbor_bytes[10:50])
        await asyncio.sleep(0.05)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(received_data_queue.get(), timeout=0.1)
        assert error_queue.empty()

        await server_raw.write(cbor_bytes[50:])
        decoded = await received_data_queue.get()
        assert decoded == obj
        assert error_queue.empty()

    async def test_cbor_stream_transformer_mixed_fragmented_and_concatenated(self, client_raw, server_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        received_data_queue = TimeoutQueue()
        client_transformed_pipe.on("data", received_data_queue.put_nowait)

        obj1 = {"id": 1, "data": "first"}
        obj2 = {"id": 2, "data": "second"}
        obj3 = {"id": 3, "data": "third"}

        cbor_bytes1 = cbor2.dumps(obj1)
        cbor_bytes2 = cbor2.dumps(obj2)
        cbor_bytes3 = cbor2.dumps(obj3)

        await server_raw.write(cbor_bytes1[:5])
        await asyncio.sleep(0.05)
        await server_raw.write(cbor_bytes1[5:])
        decoded1 = await received_data_queue.get()
        assert decoded1 == obj1

        await asyncio.sleep(0.05)
        await server_raw.write(cbor_bytes2 + cbor_bytes3)

        await server_raw.write(b"")

        decoded2 = await received_data_queue.get()
        decoded3 = await received_data_queue.get()
        assert decoded2 == obj2
        assert decoded3 == obj3

    async def test_cbor_stream_transformer_invalid_data_in_stream(self, client_raw, server_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        received_data_queue = TimeoutQueue()
        client_transformed_pipe.on("data", received_data_queue.put_nowait)
        error_queue = TimeoutQueue()
        client_transformed_pipe.on("error", error_queue.put_nowait)

        obj1 = {"valid": True}
        invalid_bytes = b"\x1f"
        obj2 = {"another": "valid"}

        await server_raw.write(cbor2.dumps(obj1))
        await server_raw.write(invalid_bytes + cbor2.dumps(obj2))

        decoded1 = await received_data_queue.get()
        assert decoded1 == obj1

        error = await asyncio.wait_for(error_queue.get(), timeout=1)
        assert isinstance(error, cbor2.CBORDecodeError)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(received_data_queue.get(), timeout=0.1)

    async def test_cbor_stream_transformer_non_bytes_data(self, client_raw, server_raw):
        transformer = CborStreamTransformer()
        with pytest.raises(TypeError):
            await transformer.decode("not cbor")

    async def test_cbor_stream_transformer_close_propagation_and_write_after_close(self, client_raw):
        cbor_stream_transformer = CborStreamTransformer()
        client_transformed_pipe = cbor_stream_transformer.apply_transformer(client_raw)

        close_queue = TimeoutQueue()
        client_transformed_pipe.on("close", lambda *args: close_queue.put_nowait(True))

        await client_raw.terminate()

        await close_queue.get()

        result = await client_transformed_pipe.write({"after": "close"})
        assert result is False


@pytest.mark.asyncio
async def test_cbor_stream_transformer_paths():
    transformer = CborStreamTransformer()

    with pytest.raises(NeedsMoreDataException):
        await transformer.decode(None)

    with pytest.raises(TypeError):
        await transformer.decode("bad")

    good = cbor2.dumps({"a": 1})
    with pytest.raises(cbor2.CBORDecodeError):
        await transformer.decode(b"\xff" + good)

    with pytest.raises(cbor2.CBORDecodeError):
        await transformer.decode(b"\xff")


@pytest.mark.asyncio
async def test_cbor_stream_transformer_overflow():
    transformer = CborStreamTransformer(max_buffer_bytes=4)

    with pytest.raises(OverflowError):
        await transformer.decode(b"12345")

