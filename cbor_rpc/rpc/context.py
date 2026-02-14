from typing import Any, Callable
from contextvars import ContextVar
from .logging import RpcLogger

current_request_id: ContextVar[Any] = ContextVar("current_request_id", default=None)


class RpcCallContext:
    def __init__(self, logger: RpcLogger, emit_progress: Callable[[Any, Any], None] = None):
        self.logger = logger
        self._emit_progress = emit_progress or (lambda v, m: None)
        self.cancelled = False

    def progress(self, value: Any, metadata: Any = None) -> None:
        self._emit_progress(value, metadata)
