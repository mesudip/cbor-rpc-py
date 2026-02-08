import asyncio
from typing import Any, List

import pytest

from cbor_rpc.pipe.pipe import Pipe


@pytest.mark.asyncio
async def test_pipe_read_timeout_returns_none():
    a, _b = Pipe.create_pair()
    result = await a.read(timeout=0.01)
    assert result is None


@pytest.mark.asyncio
async def test_pipe_cancelled_read_requeues_termination_signal():
    a, _b = Pipe.create_pair()
    read_task = asyncio.create_task(a.read(timeout=1))
    await asyncio.sleep(0)
    read_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await read_task
    a._buffer.put_nowait(None)
    result = await a.read(timeout=0.01)
    assert result is None


@pytest.mark.asyncio
async def test_pipe_terminate_closes_both_ends():
    a, b = Pipe.create_pair()
    await a.terminate("done")
    result = await b.read(timeout=0.01)
    assert result is None


@pytest.mark.asyncio
async def test_pipe_make_event_based_emits_data_and_close():
    a, b = Pipe.create_pair()
    event_pipe = a.make_event_based()
    received: List[Any] = []
    closed = asyncio.Event()

    async def on_data(data: Any) -> None:
        received.append(data)

    def on_close(*_args: Any) -> None:
        closed.set()

    event_pipe.pipeline("data", on_data)
    event_pipe.on("close", on_close)

    await b.write("hello")
    await asyncio.sleep(0.01)
    assert received == ["hello"]

    await b.terminate("bye")
    await asyncio.wait_for(closed.wait(), timeout=1)


@pytest.mark.asyncio
async def test_pipe_write_fails_when_peer_closed():
    a, b = Pipe.create_pair()
    await b.terminate("done")
    result = await a.write("data")
    assert result is False


@pytest.mark.asyncio
async def test_pipe_read_with_zero_timeout_reads_buffered_item():
    a, b = Pipe.create_pair()
    await b.write("data")
    result = await a.read(timeout=0)
    assert result == "data"


@pytest.mark.asyncio
async def test_pipe_make_event_based_write_roundtrip():
    a, b = Pipe.create_pair()
    event_pipe = a.make_event_based()
    await event_pipe.write("out")
    result = await b.read(timeout=0.1)
    assert result == "out"
    await event_pipe.terminate("done")


@pytest.mark.asyncio
async def test_pipe_make_event_based_terminate_is_idempotent(capsys):
    a, _b = Pipe.create_pair()
    event_pipe = a.make_event_based()
    await event_pipe.terminate("done")
    await event_pipe.terminate("done")
    captured = capsys.readouterr().out
    assert "PipeToEvent: Terminating event pipe." in captured
