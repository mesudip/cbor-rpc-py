from typing import Any, Generic, TypeVar
from cbor_rpc import Pipe, RpcV1, DeferredPromise

# Generic type variables
T1 = TypeVar('T1')


class SimplePipe(Pipe[T1, T1], Generic[T1]):
    def __init__(self):
        super().__init__()
        self._closed = False

    async def write(self, chunk: T1) -> bool:
        if self._closed:
            return False
        await self._emit("data", chunk)
        return True

    async def terminate(self, *args: Any) -> None:
        if self._closed:
            return
        self._closed = True
        await self._emit("close", *args)
