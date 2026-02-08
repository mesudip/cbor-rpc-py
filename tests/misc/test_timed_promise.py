import pytest

from cbor_rpc.timed_promise import TimedPromise


@pytest.mark.asyncio
async def test_timed_promise_resolve_reject_and_timeout():
    resolved = TimedPromise(10)
    await resolved.resolve("ok")
    assert await resolved.promise == "ok"

    rejected = TimedPromise(10)
    await rejected.reject("bad")
    with pytest.raises(Exception):
        await rejected.promise

    timeout_called = []

    def on_timeout():
        timeout_called.append(True)

    timed = TimedPromise(1, on_timeout)
    with pytest.raises(Exception) as exc_info:
        await timed.promise
    assert exc_info.value.args[0]["timeout"] is True
    assert timeout_called == [True]
