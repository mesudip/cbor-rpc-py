import asyncio
from typing import Any, List

import pytest

from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.event_transformer_pipe import EventTransformerPipe
from cbor_rpc.transformer.base.transformer_base import AsyncTransformer


class DummyAsyncTransformer(AsyncTransformer[Any, Any]):
    async def encode(self, data: Any) -> Any:
        if data == "encode_error":
            raise ValueError("encode boom")
        return f"enc:{data}"

    async def decode(self, data: Any) -> Any:
        if asyncio.iscoroutine(data):
            data = await data
        if data == "need_more":
            raise NeedsMoreDataException()
        if data == "decode_error":
            raise ValueError("decode boom")
        return f"dec:{data}"


@pytest.mark.asyncio
async def test_event_transformer_pipe_data_and_errors():
    base_a, base_b = EventPipe.create_inmemory_pair()

    class EventTransformer(DummyAsyncTransformer):
        async def decode(self, data: Any) -> Any:
            if data == "need_more":
                raise NeedsMoreDataException()
            if data == "decode_error":
                raise ValueError("boom")
            return await super().decode(data)

    transformer = EventTransformer()
    tpipe = EventTransformerPipe(base_a, transformer)

    received: List[Any] = []
    errors: List[Any] = []

    def on_data(data: Any) -> None:
        received.append(data)

    def on_error(err: Exception) -> None:
        errors.append(err)

    tpipe.on("data", on_data)
    tpipe.on("error", on_error)

    await base_b.write("need_more")
    await asyncio.sleep(0.01)
    assert received == []

    await base_b.write("ok")
    await asyncio.sleep(0.01)
    assert received == ["dec:ok"]

    with pytest.raises(ValueError):
        await base_b.write("decode_error")
    await asyncio.sleep(0.01)
    assert errors


@pytest.mark.asyncio
async def test_event_transformer_pipe_write_error():
    base_a, _base_b = EventPipe.create_inmemory_pair()
    transformer = DummyAsyncTransformer()
    tpipe = EventTransformerPipe(base_a, transformer)

    errors: List[Any] = []

    def on_error(err: Exception) -> None:
        errors.append(err)

    tpipe.on("error", on_error)
    ok = await tpipe.write("encode_error")
    assert ok is False
    assert errors
