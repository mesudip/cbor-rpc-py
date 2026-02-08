import asyncio
from typing import Any, List, Optional

import pytest

from cbor_rpc.pipe.aio_pipe import AioPipe


class FakeReader:
    def __init__(self, responses: List[Any]):
        self._responses = list(responses)

    async def read(self, _size: int) -> bytes:
        if not self._responses:
            return b""
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeWriter:
    def __init__(self, drain_error: Optional[Exception] = None, close_error: Optional[Exception] = None):
        self._drain_error = drain_error
        self._close_error = close_error
        self._closed = False

    def write(self, _chunk: bytes) -> None:
        return None

    async def drain(self) -> None:
        if self._drain_error:
            raise self._drain_error

    def close(self) -> None:
        if self._close_error:
            raise self._close_error
        self._closed = True

    async def wait_closed(self) -> None:
        return None


class TestAioPipe(AioPipe[bytes, bytes]):
    pass


class NotifyFailPipe(TestAioPipe):
    async def _notify(self, event_type: str, *args: Any) -> None:
        raise RuntimeError(f"notify:{event_type}")


class NotifyOncePipe(TestAioPipe):
    def __init__(self, reader: FakeReader, writer: FakeWriter):
        super().__init__(reader, writer)
        self._notified = False

    async def _notify(self, event_type: str, *args: Any) -> None:
        if event_type == "data" and not self._notified:
            self._notified = True
            raise RuntimeError("notify error")
        return await super()._notify(event_type, *args)


@pytest.mark.asyncio
async def test_aio_pipe_init_requires_reader_and_writer():
    with pytest.raises(ValueError):
        TestAioPipe(reader=FakeReader([]), writer=None)


@pytest.mark.asyncio
async def test_aio_pipe_setup_without_reader_writer():
    pipe = TestAioPipe()
    with pytest.raises(RuntimeError):
        await pipe._setup_connection()


@pytest.mark.asyncio
async def test_aio_pipe_setup_notify_error_closes():
    pipe = NotifyFailPipe(FakeReader([b"data", b""]), FakeWriter())

    with pytest.raises(RuntimeError):
        await pipe._setup_connection()

    assert pipe.is_connected() is False


@pytest.mark.asyncio
async def test_aio_pipe_read_loop_notify_error_emits_close():
    pipe = NotifyOncePipe(FakeReader([b"data", b""]), FakeWriter())
    closed = asyncio.Event()

    def on_close(*_args: Any) -> None:
        closed.set()

    pipe.on("close", on_close)
    await pipe._setup_connection()
    await asyncio.wait_for(closed.wait(), timeout=1)


@pytest.mark.asyncio
async def test_aio_pipe_read_loop_reader_error_emits_error():
    pipe = TestAioPipe(FakeReader([ValueError("boom")]), FakeWriter())
    errors: List[Any] = []

    def on_error(err: Exception) -> None:
        errors.append(err)

    pipe.on("error", on_error)
    await pipe._setup_connection()
    await asyncio.sleep(0.05)
    assert errors


@pytest.mark.asyncio
async def test_aio_pipe_write_errors():
    pipe = TestAioPipe(FakeReader([]), FakeWriter())

    with pytest.raises(ConnectionError):
        await pipe.write(b"data")

    pipe._connected = True
    with pytest.raises(TypeError):
        await pipe.write("data")

    errors: List[Any] = []

    def on_error(err: Exception) -> None:
        errors.append(err)

    pipe.on("error", on_error)
    pipe._writer = FakeWriter(drain_error=RuntimeError("drain"))
    ok = await pipe.write(b"data")
    assert ok is False
    assert errors


@pytest.mark.asyncio
async def test_aio_pipe_close_writer_exception_emits_error():
    pipe = TestAioPipe(FakeReader([]), FakeWriter())
    errors: List[Any] = []

    def on_error(err: Exception) -> None:
        errors.append(err)

    pipe.on("error", on_error)
    pipe._connected = True
    pipe._writer = FakeWriter(close_error=RuntimeError("close"))
    await pipe._close_connection()
    assert errors
