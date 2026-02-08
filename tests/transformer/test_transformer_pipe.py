import asyncio
from typing import Any, List, Optional

import pytest

from cbor_rpc.pipe.pipe import Pipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.transformer_base import AsyncTransformer
from cbor_rpc.transformer.base.transformer_pipe import TransformerPipe


class DummyPipe(Pipe[Any, Any]):
    def __init__(self):
        super().__init__()
        self._queue: asyncio.Queue = asyncio.Queue()
        self._closed = False
        self.writes: List[Any] = []
        self.terminated = False

    async def write(self, chunk: Any) -> bool:
        if self._closed:
            return False
        self.writes.append(chunk)
        return True

    async def read(self, timeout: float = None) -> Any:
        if self._closed:
            return None
        try:
            if timeout is None:
                return await self._queue.get()
            return await asyncio.wait_for(self._queue.get(), timeout)
        except asyncio.TimeoutError:
            return None

    async def terminate(self, *args: Any) -> None:
        if self._closed:
            return
        self._closed = True
        self.terminated = True
        await self._queue.put(None)
        self._emit("close", *args)

    async def push(self, item: Any) -> None:
        await self._queue.put(item)


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
async def test_transformer_pipe_read_write_and_errors():
    pipe = DummyPipe()
    transformer = DummyAsyncTransformer()
    tpipe = TransformerPipe(pipe, transformer)

    errors: List[Any] = []

    def on_error(err: Exception) -> None:
        errors.append(err)

    tpipe.on("error", on_error)

    ok = await tpipe.write("value")
    assert ok is True
    assert pipe.writes == ["enc:value"]

    ok = await tpipe.write("encode_error")
    assert ok is False
    assert errors

    await pipe.push("need_more")
    await pipe.push("data")
    result = await tpipe.read(timeout=0.1)
    assert result == "dec:data"

    await pipe.push("need_more")
    result = await tpipe.read(timeout=0.01)
    assert result is None

    await pipe.push("decode_error")
    result = await tpipe.read(timeout=0.1)
    assert result is None

    await tpipe.terminate()
    assert pipe._closed is True


@pytest.mark.asyncio
async def test_transformer_pipe_error_event_closes_pipe():
    pipe = DummyPipe()
    transformer = DummyAsyncTransformer()
    tpipe = TransformerPipe(pipe, transformer)

    pipe._emit("error", RuntimeError("boom"))

    await asyncio.sleep(0)
    assert tpipe._closed is True


@pytest.mark.asyncio
async def test_transformer_pipe_write_encode_error_emits_error():
    class BadEncodeTransformer:
        async def encode(self, value: Any) -> Any:
            raise ValueError("encode-fail")

        async def decode(self, value: Any) -> Any:
            return value

    pipe = DummyPipe()
    transformer = TransformerPipe(pipe, BadEncodeTransformer())
    errors: List[str] = []

    def on_error(err: Exception) -> None:
        errors.append(str(err))

    transformer.on("error", on_error)
    result = await transformer.write("data")
    assert result is False
    assert errors == ["encode-fail"]


@pytest.mark.asyncio
async def test_transformer_pipe_read_needs_more_data_timeout():
    class NeedMoreTransformer:
        async def encode(self, value: Any) -> Any:
            return value

        async def decode(self, value: Any) -> Any:
            raise NeedsMoreDataException()

    pipe = DummyPipe()
    transformer = TransformerPipe(pipe, NeedMoreTransformer())
    await pipe.push("chunk")
    result = await transformer.read(timeout=0)
    assert result is None


@pytest.mark.asyncio
async def test_transformer_pipe_read_decode_error_emits_error():
    class BadDecodeTransformer:
        async def encode(self, value: Any) -> Any:
            return value

        async def decode(self, value: Any) -> Any:
            raise ValueError("decode-fail")

    pipe = DummyPipe()
    transformer = TransformerPipe(pipe, BadDecodeTransformer())
    errors: List[str] = []

    def on_error(err: Exception) -> None:
        errors.append(str(err))

    transformer.on("error", on_error)
    await pipe.push("chunk")
    result = await transformer.read(timeout=0.1)
    assert result is None
    assert errors == ["decode-fail"]


@pytest.mark.asyncio
async def test_transformer_pipe_close_error_propagation_and_terminate():
    class PassThroughTransformer:
        async def encode(self, value: Any) -> Any:
            return value

        async def decode(self, value: Any) -> Any:
            return value

    pipe = DummyPipe()
    transformer = TransformerPipe(pipe, PassThroughTransformer())
    closed = asyncio.Event()
    errors: List[str] = []
    propagated: List[str] = []

    def on_close(*_args: Any) -> None:
        closed.set()

    def on_error(err: Exception) -> None:
        errors.append(str(err))

    def on_pipe_error(err: Exception) -> None:
        propagated.append(str(err))

    transformer.on("close", on_close)
    transformer.on("error", on_error)
    pipe.on("error", on_pipe_error)

    pipe._emit("error", Exception("pipe-error"))
    await asyncio.sleep(0.01)
    assert errors == ["pipe-error"]
    assert transformer._closed is True

    transformer._propagate_error(Exception("propagate"))
    assert propagated[-1:] == ["propagate"]

    await transformer.terminate()
    await transformer.terminate()
    assert pipe.terminated is True

    pipe._emit("close", "done")
    await asyncio.wait_for(closed.wait(), timeout=1)
