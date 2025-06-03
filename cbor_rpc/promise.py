from typing import Any
import asyncio


class DeferredPromise:
    def __init__(self, timeout_ms: int, message: str = "Timeout on RPC call"):
        self._future = asyncio.get_event_loop().create_future()
        self._timeout_handle = asyncio.get_event_loop().call_later(
            timeout_ms / 1000.0,
            lambda: self._future.set_exception(Exception({"timeout": True, "timeoutPeriod": timeout_ms, "message": message}))
            if not self._future.done() else None
        )

    @property
    def promise(self) -> asyncio.Future:
        return self._future

    async def resolve(self, result: Any) -> None:
        if self._timeout_handle:
            self._timeout_handle.cancel()
            if not self._future.done():
                self._future.set_result(result)

    async def reject(self, error: Any) -> None:
        if self._timeout_handle:
            self._timeout_handle.cancel()
            if not self._future.done():
                self._future.set_exception(Exception(error))
