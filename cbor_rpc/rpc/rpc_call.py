from typing import Any, Callable, Optional, List
from asyncio import Future
from cbor_rpc.timed_promise import TimedPromise


class RpcCallHandle:
    """
    Represents an active RPC call session.
    Allows listening for logs, progress updates, and cancelling the call.
    """

    def __init__(self, id_: int, promise: TimedPromise, pipe: Any):
        self._id = id_
        self._promise = promise
        self._pipe = pipe
        self._log_listeners: List[Callable[[int, Any], None]] = []
        self._progress_listeners: List[Callable[[Any, Any], None]] = []
        self._last_progress: Optional[Any] = None
        self._last_progress_metadata: Optional[Any] = None

    @property
    def id(self) -> int:
        return self._id

    @property
    def result(self) -> Future:
        """The future that resolves to the RPC result."""
        return self._promise.promise

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
        # If we already have progress, maybe valid to emit?
        # Usually listeners are attached before execution starts or during.
        # But if attached late, might miss previous events.
        # For now, just append.
        return self

    def get_progress(self) -> Any:
        """Returns the last received progress value."""
        return self._last_progress

    def cancel(self) -> None:
        """Cancels the RPC call."""
        self._cancel_cb()

        # We might also want to reject the promise locally if we desire fast fail
        # but typically we wait for server ack or just timeout.
        # However, for UI responsiveness, we might want to fail the promise with CancelledError?
        # The prompt says "Client should be able to ... send cancel()".
        pass

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

    def cancel(self) -> None:
        """Cancel the RPC call."""
        import asyncio

        asyncio.create_task(self._pipe.write([1, 3, self._id]))
