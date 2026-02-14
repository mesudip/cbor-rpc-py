from typing import Any, Callable, Optional
import asyncio
import sys
from cbor_rpc.pipe.event_pipe import EventPipe


class RpcLogger:
    def __init__(
        self,
        pipe: Optional[EventPipe],
        id_provider: Callable[[], Any],
        min_level: int = 5,
    ):
        self.pipe = pipe
        self._id_provider = id_provider
        self.min_level = min_level

    def _write(self, level: int, content: Any) -> None:
        async def do_write():
            if level > self.min_level:
                return

            rpc_id = self._id_provider()

            if self.pipe:
                await self.pipe.write([2, 0, rpc_id, level, content])
            else:
                # Fallback to console
                print(f"[RPC Log {rpc_id} - L{level}] {content}", file=sys.stderr)

        asyncio.create_task(do_write())

    def log(self, content: Any) -> None:
        self._write(3, content)  # Info/Log

    def info(self, content: Any) -> None:
        self._write(3, content)  # Info/Log

    def warn(self, content: Any) -> None:
        self._write(2, content)  # Warning

    def crit(self, content: Any) -> None:
        self._write(1, content)  # Critical

    def verbose(self, content: Any) -> None:
        self._write(4, content)  # Verbose

    def debug(self, content: Any) -> None:
        self._write(5, content)  # Debug
