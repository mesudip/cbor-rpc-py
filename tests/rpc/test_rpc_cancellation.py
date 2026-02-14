import asyncio
import pytest
from typing import Any, List
from cbor_rpc import RpcV1
from cbor_rpc.rpc.context import RpcCallContext
from tests.helpers import SimplePipe


@pytest.mark.asyncio
async def test_rpc_cancellation():
    pipe = SimplePipe()
    cancelled_event = asyncio.Event()

    async def long_running_op(context: RpcCallContext, delay: float):
        try:
            # Poll cancelled status
            for _ in range(int(delay * 20)):
                if context.cancelled:
                    cancelled_event.set()
                    return "Cancelled"
                await asyncio.sleep(0.05)
            return "Completed"
        except Exception:
            raise

    def method_handler(context: RpcCallContext, method: str, args: List[Any]) -> Any:
        if method == "long_op":
            return long_running_op(context, *args)
        return None

    rpc = RpcV1.make_rpc_v1(pipe, "self", method_handler)

    # Make call
    call_handle = rpc.create_call("long_op", 1.0)

    # Wait a bit then cancel
    await asyncio.sleep(0.1)
    call_handle.cancel()

    # Wait for server to detect cancellation
    try:
        await asyncio.wait_for(cancelled_event.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("Server did not detect cancellation")

    res = await call_handle.result
    # The server function returns "Cancelled" when it detects the flag
    assert res == "Cancelled"
