import pytest
import asyncio
from typing import Any, Callable
from cbor_rpc.event.emitter import AbstractEmitter

@pytest.mark.asyncio
async def test_on_and_emit():
    """Test that subscribers registered with 'on' are called by '_emit' in registration order."""
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

    # Register subscribers
    emitter.on("test", async_handler1)
    emitter.on("test", sync_handler1)
    emitter.on("test", async_handler2)

    # Emit event
    emitter._emit("test", "event1")
    await asyncio.sleep(0.02)  # Allow async handlers to complete

    # Verify all subscribers ran (order may vary due to concurrency)
    expected = [
        f"async_handler1_event1",
        f"sync_handler1_event1",
        f"async_handler2_event1"
    ]
    assert sorted(events) == sorted(expected), f"Expected {expected}, got {events}"

@pytest.mark.asyncio
async def test_pipeline_and_notify():
    """Test that pipelines run before subscribers in '_notify', respecting registration order."""
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    async def async_pipeline1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline1_{data}")

    def sync_pipeline1(data: Any):
        events.append(f"sync_pipeline1_{data}")

    async def async_pipeline2(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline2_{data}")

    # Register handlers and pipelines
    emitter.on("test", async_handler1)
    emitter.on("test", sync_handler1)
    emitter.pipeline("test", async_pipeline1)
    emitter.pipeline("test", sync_pipeline1)
    emitter.pipeline("test", async_pipeline2)

    # Notify event
    await emitter._notify("test", "event2")
    await asyncio.sleep(0.02)  # Allow async pipelines and handlers to complete

    # Verify pipelines run before subscribers
    expected_pipelines = [
        f"async_pipeline1_event2",
        f"sync_pipeline1_event2",
        f"async_pipeline2_event2"
    ]
    expected_subscribers = [
        f"async_handler1_event2",
        f"sync_handler1_event2"
    ]
    pipeline_indices = [events.index(e) for e in expected_pipelines if e in events]
    subscriber_indices = [events.index(e) for e in expected_subscribers if e in events]
    assert all(p < s for p in pipeline_indices for s in subscriber_indices), \
        f"Pipelines {expected_pipelines} should precede subscribers {expected_subscribers} in {events}"
    assert sorted(events) == sorted(expected_pipelines + expected_subscribers), \
        f"Expected {expected_pipelines + expected_subscribers}, got {events}"

@pytest.mark.asyncio
async def test_unsubscribe():
    """Test that unsubscribing a handler removes it from the subscriber list."""
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    # Register and unsubscribe
    emitter.on("test", async_handler1)
    emitter.on("test", sync_handler1)
    emitter.unsubscribe("test", async_handler1)

    # Emit event
    emitter._emit("test", "event3")
    await asyncio.sleep(0.02)  # Allow async handlers to complete (none in this case)

    # Verify only remaining subscriber ran
    expected = [f"sync_handler1_event3"]
    assert events == expected, f"Expected {expected}, got {events}"

@pytest.mark.asyncio
async def test_replace_on_handler():
    """Test that replace_on_handler sets only the new handler for the event."""
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    # Register and replace
    emitter.on("test", sync_handler1)
    emitter.replace_on_handler("test", async_handler1)

    # Emit event
    emitter._emit("test", "event4")
    await asyncio.sleep(0.02)  # Allow async handler to complete

    # Verify only the replaced handler ran
    expected = [f"async_handler1_event4"]
    assert events == expected, f"Expected {expected}, got {events}"

@pytest.mark.asyncio
async def test_pipeline_failure():
    """Test that '_notify' raises an exception if a pipeline fails and doesn't call subscribers."""
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_pipeline1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline1_{data}")
        raise ValueError("Pipeline failed")

    def sync_handler1(data: Any):
        events.append(f"sync_handler1_{data}")

    # Register pipeline and subscriber
    emitter.pipeline("test", async_pipeline1)
    emitter.on("test", sync_handler1)

    # Notify with failing pipeline
    try:
        await emitter._notify("test", "event5")
        assert False, "Exception not thrown"
    except ValueError:
        pass

    # Verify only the pipeline ran, not the subscriber
    expected = [f"async_pipeline1_event5"]
    assert events == expected, f"Expected {expected}, got {events}"

@pytest.mark.asyncio
async def test_multiple_event_types():
    """Test that only handlers for the triggered event type are called."""
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    # Handlers for event_a
    async def async_handler_a(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler_a_{data}")

    def sync_handler_a(data: Any):
        events.append(f"sync_handler_a_{data}")

    # Handlers for event_b
    async def async_handler_b(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler_b_{data}")

    def sync_handler_b(data: Any):
        events.append(f"sync_handler_b_{data}")

    # Pipelines for event_a
    async def async_pipeline_a(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_pipeline_a_{data}")

    # Pipelines for event_b
    def sync_pipeline_b(data: Any):
        events.append(f"sync_pipeline_b_{data}")

    # Register handlers and pipelines for different event types
    emitter.on("event_a", async_handler_a)
    emitter.on("event_a", sync_handler_a)
    emitter.pipeline("event_a", async_pipeline_a)
    emitter.on("event_b", async_handler_b)
    emitter.on("event_b", sync_handler_b)
    emitter.pipeline("event_b", sync_pipeline_b)

    # Test _emit for event_a
    events.clear()
    emitter._emit("event_a", "data_a")
    await asyncio.sleep(0.02)  # Allow async handlers to complete
    expected = [f"async_handler_a_data_a", f"sync_handler_a_data_a"]
    assert sorted(events) == sorted(expected), f"Expected {expected}, got {events}"

    # Test _notify for event_a
    events.clear()
    await emitter._notify("event_a", "data_a2")
    await asyncio.sleep(0.02)  # Allow async pipelines and handlers to complete
    expected_pipelines = [f"async_pipeline_a_data_a2"]
    expected_subscribers = [f"async_handler_a_data_a2", f"sync_handler_a_data_a2"]
    pipeline_indices = [events.index(e) for e in expected_pipelines if e in events]
    subscriber_indices = [events.index(e) for e in expected_subscribers if e in events]
    assert all(p < s for p in pipeline_indices for s in subscriber_indices), \
        f"Pipelines {expected_pipelines} should precede subscribers {expected_subscribers} in {events}"
    assert sorted(events) == sorted(expected_pipelines + expected_subscribers), \
        f"Expected {expected_pipelines + expected_subscribers}, got {events}"

    # Test _emit for event_b
    events.clear()
    emitter._emit("event_b", "data_b")
    await asyncio.sleep(0.02)  # Allow async handlers to complete
    expected = [f"async_handler_b_data_b", f"sync_handler_b_data_b"]
    assert sorted(events) == sorted(expected), f"Expected {expected}, got {events}"

    # Test _notify for event_b
    events.clear()
    await emitter._notify("event_b", "data_b2")
    await asyncio.sleep(0.02)  # Allow async handlers to complete
    expected_pipelines = [f"sync_pipeline_b_data_b2"]
    expected_subscribers = [f"async_handler_b_data_b2", f"sync_handler_b_data_b2"]
    pipeline_indices = [events.index(e) for e in expected_pipelines if e in events]
    subscriber_indices = [events.index(e) for e in expected_subscribers if e in events]
    assert all(p < s for p in pipeline_indices for s in subscriber_indices), \
        f"Pipelines {expected_pipelines} should precede subscribers {expected_subscribers} in {events}"
    assert sorted(events) == sorted(expected_pipelines + expected_subscribers), \
        f"Expected {expected_pipelines + expected_subscribers}, got {events}"

@pytest.mark.asyncio
async def test_background_task_failure():
    """Test that background task failures in '_emit' don't affect other subscribers."""
    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    async def async_handler1(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler1_{data}")

    async def async_handler2(data: Any):
        await asyncio.sleep(0.01)
        events.append(f"async_handler2_{data}")
        raise ValueError("async_handler2 failed")

    def sync_handler(data: Any):
        events.append(f"sync_handler_{data}")

    # Register subscribers, including one that fails
    emitter.on("test", async_handler1)
    emitter.on("test", async_handler2)
    emitter.on("test", sync_handler)

    # Emit event
    emitter._emit("test", "event6")
    await asyncio.sleep(0.02)  # Allow async handlers to complete

    # Verify all subscribers ran despite the failure
    expected = [
        f"async_handler1_event6",
        f"async_handler2_event6",
        f"sync_handler_event6"
    ]
    assert sorted(events) == sorted(expected), f"Expected {expected}, got {events}"

@pytest.mark.asyncio
async def test_slow_emit_does_not_block_notify():
    """Test that a slow handler in _emit does not block a subsequent _notify call."""

    class TestEmitter(AbstractEmitter):
        pass

    emitter = TestEmitter()
    events = []

    # Slow async handler for _emit
    async def slow_handler(data: Any):
        await asyncio.sleep(0.8)
        events.append(f"slow_handler_{data}")

    # Fast handler for _notify
    def fast_notify_handler(data: Any):
        events.append(f"fast_notify_handler_{data}")

    def fast_notify_pipeline(data: Any):
        events.append(f"fast_notify_pipeline_{data}")

    # Register handlers
    emitter.on("test_emit", slow_handler)
    emitter.on("test_notify", fast_notify_handler)
    emitter.pipeline("test_notify", fast_notify_pipeline)

    # Start _emit but don't wait for it to finish
    emitter._emit("test_emit", "data_emit")

    # Wait briefly before triggering _notify
    await asyncio.sleep(0.1)

    # Trigger and await _notify
    await emitter._notify("test_notify", "data_notify")

    # Give time for _notify to complete before slow_handler finishes
    await asyncio.sleep(0.4)

    # ✅ Confirm that notify events are already present
    assert "fast_notify_pipeline_data_notify" in events
    assert "fast_notify_handler_data_notify" in events
    assert "slow_handler_data_emit" not in events  # not yet complete

    # Wait for slow handler to complete
    await asyncio.sleep(0.5)

    # Now validate the full event order
    slow_index = events.index("slow_handler_data_emit")
    pipeline_index = events.index("fast_notify_pipeline_data_notify")
    handler_index = events.index("fast_notify_handler_data_notify")

    assert pipeline_index < slow_index and handler_index < slow_index, (
        f"_notify handlers [{pipeline_index}, {handler_index}] should run before slow _emit [{slow_index}]"
    )

    expected = {
        "fast_notify_pipeline_data_notify",
        "fast_notify_handler_data_notify",
        "slow_handler_data_emit"
    }
    assert set(events) == expected, f"Expected {expected}, got {set(events)}"
