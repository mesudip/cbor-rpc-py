import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .transformer_base import Transformer
from typing import Any, Awaitable, Callable, TypeVar

from cbor_rpc.pipe import EventPipe
from .base_exception import NeedsMoreDataException

T1 = TypeVar("T1")  # Output type after decoding
T2 = TypeVar("T2")  # Input type before decoding (pipe input/output type)


class EventTransformerPipe(EventPipe[T1, T2]):
    encode: Callable[[T1], Awaitable[T2]]
    decode: Callable[[T2], Awaitable[T1]]

    def __init__(self, pipe: EventPipe[T2, T2], transformer: "Transformer"):
        super().__init__()
        self.pipe = pipe
        self.pipe.pipeline("data", self._handle_data)
        self.pipe.on("close", self._on_close)
        self.pipe.on("error", self._on_error)
        if transformer:
            self.encode = transformer.encode
            self.decode = transformer.decode
        else:
            raise ValueError("A transformer must be provided or encode/decode must be overridden")

    async def _handle_data(self, data: T2):
        try:
            decoded = await self.decode(data)
            await self._notify("data", decoded)
        except NeedsMoreDataException:
            # If more data is needed, simply return and wait for the next chunk
            return
        except Exception as e:
            # Let the exception propagate up to AbstractEmitter._notify,
            # which will catch it and emit the "error" event.
            raise e

        # Try to decode more from the buffer if the transformer supports it
        while True:
            try:
                # We pass None to indicate "no new data, just decode from buffer"
                # We catch TypeError in case the transformer does not support None/buffering
                decoded = await self.decode(None)
                await self._notify("data", decoded)
            except NeedsMoreDataException:
                break
            except TypeError:
                # Transformer likely doesn't support None (not a stream transformer)
                break
            except Exception as e:
                # Other errors in subsequent decoding should probably be reported.
                # However, since the *primary* data was processed, maybe we should just emit error?
                # or raise? Raising here might be outside the context of the initial caller if we were async...
                # But we are inside _handle_data which is awaited by _notify.
                raise e

    def _on_close(self, *args: Any):
        self._emit("close", *args)

    def _on_error(self, error: Exception):
        self._emit("error", error)

    async def write(self, chunk: T1) -> bool:
        try:
            encoded = await self.encode(chunk)
            return await self.pipe.write(encoded)
        except Exception as e:
            self._emit("error", e)
            return False

    async def terminate(self, *args: Any) -> None:
        await self.pipe.terminate(*args)
