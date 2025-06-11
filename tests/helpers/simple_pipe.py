from typing import Any, Generic, TypeVar
from cbor_rpc import EventPipe, RpcV1, TimedPromise

# Generic type variables
T1 = TypeVar('T1')


class SimplePipe(EventPipe[T1, T1], Generic[T1]):
    def __init__(self):
        super().__init__()
        self._closed = False

    async def write(self, chunk: T1) -> bool:
        if self._closed:
            return False
        await self._notify("data", chunk)
        return True

    async def terminate(self, *args: Any) -> None:
        if self._closed:
            return
        self._closed = True
        self._emit("close", *args)
