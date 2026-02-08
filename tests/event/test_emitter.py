import asyncio
import time
from typing import Any

import pytest

from cbor_rpc.event.emitter import AbstractEmitter


@pytest.mark.asyncio
async def test_on_and_emit():
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    async def async_handler2(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler2_{data}")

    emitter.on("test", async_handler1)
    emitter.on("test", sync_handler1)
    emitter.on("test", async_handler2)

    emitter._emit("test", "event1")
    await asyncio.sleep(0.02)

    expected = [
        "async_handler1_event1",
        "sync_handler1_event1",
        "async_handler2_event1",
    ]
    assert sorted(events) == sorted(expected), f"Expected {expected}, got {events}"


@pytest.mark.asyncio
async def test_pipeline_and_notify():
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    def sync_handler1(data: Any):
        time.sleep(0.01)
        events.append(f"sync_handler1_{data}")

    async def async_pipeline1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline1_{data}")

    async def async_pipeline2(data: Any):
        # Slow pipeline
        await asyncio.sleep(0.05)
        events.append(f"async_pipeline2_{data}")

    async def async_pipeline3(data: Any):
        # Fast pipeline
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline3_{data}")

    emitter.on("test", async_handler1)
    emitter.on("test", sync_handler1)
    emitter.pipeline("test", async_pipeline1)
    emitter.pipeline("test", async_pipeline2)
    emitter.pipeline("test", async_pipeline3)

    await emitter._notify("test", "event2")
    await asyncio.sleep(0.02)

    # Strictly sequential execution order:
    # 1. async_pipeline1 (waits 0.01)
    # 2. async_pipeline2 (waits 0.05) -> If concurrent, this would finish LAST
    # 3. async_pipeline3 (waits 0.01) -> If concurrent, this would finish BEFORE pipeline2

    expected_pipelines = [
        "async_pipeline1_event2",
        "async_pipeline2_event2",
        "async_pipeline3_event2",
    ]

    # Verify pipelines ran in strict order
    assert events[:3] == expected_pipelines, f"Pipelines expected {expected_pipelines}, got {events[:3]}"

    expected_subscribers = ["async_handler1_event2", "sync_handler1_event2"]

    # Verify handlers ran after pipelines
    assert set(events[3:]) == set(
        expected_subscribers
    ), f"Subscribers expected {expected_subscribers}, got {events[3:]}"

    # Explicitly assert precedence
    for p in expected_pipelines:
        for s in expected_subscribers:
            assert events.index(p) < events.index(s), f"Pipeline {p} executed after subscriber {s}"


@pytest.mark.asyncio
async def test_unsubscribe():
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    emitter.on("test", async_handler1)
    emitter.on("test", sync_handler1)
    emitter.unsubscribe("test", async_handler1)

    emitter._emit("test", "event3")
    await asyncio.sleep(0.02)

    expected = ["sync_handler1_event3"]
    assert events == expected, f"Expected {expected}, got {events}"


@pytest.mark.asyncio
async def test_replace_on_handler():
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    emitter.on("test", sync_handler1)
    emitter.replace_on_handler("test", async_handler1)

    emitter._emit("test", "event4")
    await asyncio.sleep(0.02)

    expected = ["async_handler1_event4"]
    assert events == expected, f"Expected {expected}, got {events}"


@pytest.mark.asyncio
async def test_pipeline_failure():
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_pipeline1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline1_{data}")
        raise ValueError("Pipeline failed")

    async def async_pipeline2(data: Any):
        events.append(f"async_pipeline2_{data}")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    emitter.pipeline("test", async_pipeline1)
    emitter.pipeline("test", async_pipeline2)
    emitter.on("test", sync_handler1)

    with pytest.raises(ValueError):
        await emitter._notify("test", "event5")

    expected = ["async_pipeline1_event5"]
    assert events == expected, f"Expected {expected}, got {events}"

    # Explicitly verify skipped execution
    assert "async_pipeline2_event5" not in events, "Subsequent pipeline should be skipped"
    assert "sync_handler1_event5" not in events, "Event subscribers should be skipped"


@pytest.mark.asyncio
async def test_multiple_event_types():
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler_a(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler_a_{data}")

    def sync_handler_a(data: Any):
        events.append(f"sync_handler_a_{data}")

    async def async_handler_b(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler_b_{data}")

    def sync_handler_b(data: Any):
        events.append(f"sync_handler_b_{data}")

    async def async_pipeline_a(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline_a_{data}")

    def sync_pipeline_b(data: Any):
        events.append(f"sync_pipeline_b_{data}")

    emitter.on("event_a", async_handler_a)
    emitter.on("event_a", sync_handler_a)
    emitter.pipeline("event_a", async_pipeline_a)
    emitter.on("event_b", async_handler_b)
    emitter.on("event_b", sync_handler_b)
    emitter.pipeline("event_b", sync_pipeline_b)

    events.clear()
    emitter._emit("event_a", "data_a")
    await asyncio.sleep(0.02)
    expected = ["async_handler_a_data_a", "sync_handler_a_data_a"]
    assert sorted(events) == sorted(expected), f"Expected {expected}, got {events}"

    events.clear()
    await emitter._notify("event_a", "data_a2")
    await asyncio.sleep(0.02)
    expected_pipelines = ["async_pipeline_a_data_a2"]
    expected_subscribers = ["async_handler_a_data_a2", "sync_handler_a_data_a2"]
    pipeline_indices = [events.index(e) for e in expected_pipelines if e in events]
    subscriber_indices = [events.index(e) for e in expected_subscribers if e in events]
    assert all(
        p < s for p in pipeline_indices for s in subscriber_indices
    ), f"Pipelines {expected_pipelines} should precede subscribers {expected_subscribers} in {events}"
    assert sorted(events) == sorted(
        expected_pipelines + expected_subscribers
    ), f"Expected {expected_pipelines + expected_subscribers}, got {events}"


class DummyEmitter(AbstractEmitter):
    pass


def test_emitter_no_running_loop_warning():
    emitter = DummyEmitter()
    with pytest.warns(RuntimeWarning):
        emitter._run_background_task(lambda: None)


def test_emitter_emit_sync_error_warning():
    emitter = DummyEmitter()

    def bad_handler(_data: Any) -> None:
        raise ValueError("boom")

    emitter.on("evt", bad_handler)
    with pytest.warns(RuntimeWarning):
        emitter._emit("evt", "data")


@pytest.mark.asyncio
async def test_emitter_notify_pipeline_errors():
    emitter = DummyEmitter()
    errors = []

    def on_error(err: Exception) -> None:
        errors.append(err)

    def bad_pipeline(_data: Any) -> None:
        raise ValueError("boom")

    emitter.on("error", on_error)
    emitter.pipeline("evt", bad_pipeline)

    with pytest.raises(ValueError):
        await emitter._notify("evt", "data")
    assert errors


@pytest.mark.asyncio
async def test_emitter_notify_async_pipeline_error():
    emitter = DummyEmitter()
    errors = []

    async def bad_pipeline(_data: Any) -> None:
        raise ValueError("boom")

    def on_error(err: Exception) -> None:
        errors.append(err)

    emitter.on("error", on_error)
    emitter.pipeline("evt", bad_pipeline)

    with pytest.raises(ValueError):
        await emitter._notify("evt", "data")
    assert errors
