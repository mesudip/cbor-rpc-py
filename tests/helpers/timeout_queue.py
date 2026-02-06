import asyncio
from typing import Any, Optional


class TimeoutQueue(asyncio.Queue):
    def __init__(self, *args: Any, default_timeout: float = 1.0, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._default_timeout = default_timeout

    async def get(self, timeout: Optional[float] = None) -> Any:
        if timeout is None:
            timeout = self._default_timeout
        return await asyncio.wait_for(super().get(), timeout=timeout)
