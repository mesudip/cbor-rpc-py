from typing import Any, Callable, Optional, List
from asyncio import Future
from cbor_rpc.timed_promise import TimedPromise
import asyncio


class RpcCallHandle:
    """
    Represents an active RPC call session.
    Allows listening for logs, progress updates, and cancelling the call.
    """

    def __init__(
        self,
        id_: int,
        pipe: Any,
        method: str,
        args: List[Any],
        timeout: int,
        start_callback: Callable[["RpcCallHandle"], TimedPromise],
    ):
        self._id = id_
        self._pipe = pipe
        self._method = method
        self._args = args
        self._timeout = timeout
        self._start_callback = start_callback
        self._promise: Optional[TimedPromise] = None
        self._log_listeners: List[Callable[[int, Any], None]] = []
        self._progress_listeners: List[Callable[[Any, Any], None]] = []
        self._last_progress: Optional[Any] = None
        self._last_progress_metadata: Optional[Any] = None

    @property
    def id(self) -> int:
        return self._id

    async def result(self) -> Any:
        """
        The future that resolves to the RPC result.
        Triggers the call if it hasn't been started yet.
        """
        if self._promise is None:
            self.call()
        if self._promise is None:
            raise RuntimeError("Call could not be started.")
        return await self._promise.promise

    def __await__(self):
        return self.result().__await__()

    def set_timeout(self, timeout_ms: int) -> "RpcCallHandle":
        if self._promise is not None:
            raise RuntimeError("Cannot set timeout after call has started.")
        self._timeout = timeout_ms

    def call(self) -> "RpcCallHandle":
        if self._promise is not None:
            return self
        self._promise = self._start_callback(self)

    def on_log(self, callback: Callable[[int, Any], None]) -> "RpcCallHandle":
        """
        Register a callback for log messages.
        Callback signature: (level: int, content: Any)
        """
        self._log_listeners.append(callback)
        return self

    def on_progress(self, callback: Callable[[Any, Any], None]) -> "RpcCallHandle":
        """
        Register a callback for progress updates.
        Callback signature: (value: Any, metadata: Any)
        """
        self._progress_listeners.append(callback)

    def get_progress(self) -> Any:
        """Returns the last received progress value."""
        return self._last_progress

    # Internal methods called by RpcV1

    def _emit_log(self, level: int, content: Any) -> None:
        for listener in self._log_listeners:
            try:
                listener(level, content)
            except Exception:
                # User callback failed, ignore to keep system stable
                pass

    def _emit_progress(self, value: Any, metadata: Any) -> None:
        self._last_progress = value
        self._last_progress_metadata = metadata
        for listener in self._progress_listeners:
            try:
                listener(value, metadata)
            except Exception:
                pass

    async def cancel(self) -> None:
        """Cancel the RPC call."""
        await self._pipe.write([1, 3, self._id])
