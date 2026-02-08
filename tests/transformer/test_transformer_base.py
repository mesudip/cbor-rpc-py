import pytest

from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.pipe.pipe import Pipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.event_transformer_pipe import EventTransformerPipe
from cbor_rpc.transformer.base.transformer_base import AsyncTransformer, Transformer
from cbor_rpc.transformer.base.transformer_pipe import TransformerPipe


class DummyAsyncTransformer(AsyncTransformer):
    async def encode(self, data):
        return f"enc:{data}"

    async def decode(self, data):
        return f"dec:{data}"


class DummySyncTransformer(Transformer):
    def encode(self, data):
        return f"enc:{data}"

    def decode(self, data):
        return f"dec:{data}"


class DummyPipe(Pipe):
    async def write(self, _chunk):
        return True

    async def read(self, _timeout=None):
        return None

    async def terminate(self, *args):
        return None


def test_transformer_base_apply_transformer_invalid_type():
    transformer = DummySyncTransformer()
    with pytest.raises(TypeError):
        transformer.apply_transformer(object())


def test_transformer_base_apply_transformer_variants():
    transformer = DummySyncTransformer()
    pipe = DummyPipe()
    event_pipe_a, _event_pipe_b = EventPipe.create_inmemory_pair()

    bound_pipe = transformer.apply_transformer(pipe)
    bound_event_pipe = transformer.apply_transformer(event_pipe_a)

    assert isinstance(bound_pipe, TransformerPipe)
    assert isinstance(bound_event_pipe, EventTransformerPipe)


def test_transformer_base_wait_next_data_raises():
    transformer = DummySyncTransformer()
    async_transformer = DummyAsyncTransformer()
    with pytest.raises(NeedsMoreDataException):
        transformer.wait_next_data()
    with pytest.raises(NeedsMoreDataException):
        async_transformer.wait_next_data()
