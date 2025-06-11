from typing import Any, Callable, Optional
import asyncio


class TimedPromise:
    def __init__(self, timeout_ms: int, timeout_cb: Optional[Callable[[], None]] = None, 
                 message: str = "Timeout on RPC call"):
        self._timeout_ms = timeout_ms
        self._timeout_cb = timeout_cb
        self._message = message
        self._future = asyncio.get_event_loop().create_future()
        self._timeout_handle = None
        self._resolved = False
        
        # Set up timeout
        if timeout_ms > 0:
            self._timeout_handle = asyncio.get_event_loop().call_later(
                timeout_ms / 1000.0,
                self._on_timeout
            )

    @property
    def promise(self) -> asyncio.Future:
        return self._future

    async def resolve(self, result: Any) -> None:
        if not self._resolved and not self._future.done():
            self._resolved = True
            self._clear_timeout()
            self._future.set_result(result)

    async def reject(self, error: Any) -> None:
        if not self._resolved and not self._future.done():
            self._resolved = True
            self._clear_timeout()
            self._future.set_exception(Exception(error))

    def _clear_timeout(self) -> None:
        if self._timeout_handle:
            self._timeout_handle.cancel()
            self._timeout_handle = None

    def _on_timeout(self) -> None:
        if not self._resolved and not self._future.done():
            self._resolved = True
            error_data = {
                "timeout": True,
                "timeoutPeriod": self._timeout_ms,
                "message": self._message
            }
            self._future.set_exception(Exception(error_data))
            if self._timeout_cb:
                self._timeout_cb()
