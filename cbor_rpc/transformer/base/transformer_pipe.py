from typing import Any, Awaitable, Optional, TypeVar, Callable
from typing import TYPE_CHECKING
import time

from .base_exception import NeedsMoreDataException
if TYPE_CHECKING:
    from .transformer_base import Transformer
from cbor_rpc.pipe.pipe import Pipe

T1 = TypeVar("T1")
T2 = TypeVar("T2")

class TransformerPipe(Pipe[T1, T2]):
    encode: Callable[[T1], Awaitable[T2]]
    decode: Callable[[T2], Awaitable[T1]]

    def __init__(self, pipe: Pipe[Any, Any], transformer: 'Optional[Transformer[T1, T2]]' ):
        super().__init__()
        self.pipe = pipe

        self.encode = transformer.encode
        self.decode = transformer.decode

        def _handle_error(*args):
            if not self._closed:
                self._closed = True
                self.pipe.terminate()
            self._emit('error', *args)

        def _handle_close(*args):
            if not self._closed:
                self._closed = True
            self._emit('close', *args)

        self.pipe.on('error', _handle_error)
        self.pipe.on('close', _handle_close)

    async def write(self, chunk: T1) -> bool:
        if self._closed:
            return False
        try:
            encoded = await self.encode(chunk)
            return self.pipe.write(encoded)
        except Exception:
            self._emit('error', chunk)
            return False

    async def read(self, timeout: Optional[float] = None) -> Optional[T1]:
        if self._closed:
            return None

        start = time.monotonic()
        remaining = timeout

        try:
            while True:
                raw = self.pipe.read(remaining)
                if raw is None:
                    return None

                try:
                    return await self.decode(raw)
                except NeedsMoreDataException:
                    if timeout is not None:
                        elapsed = time.monotonic() - start
                        remaining = max(0, timeout - elapsed)
                        if remaining == 0:
                            return None
        except Exception as e:
            self._emit('error', e)
            return None

    def terminate(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.pipe.terminate()

    def _propagate_error(self, *args):
        self.pipe._emit('error', *args)
