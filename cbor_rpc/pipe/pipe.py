from abc import ABC, abstractmethod
from typing import Any, TypeVar, Generic, Optional, Tuple
import asyncio

from cbor_rpc.pipe.event_pipe import EventPipe
from ..event.emitter import AbstractEmitter

T1 = TypeVar("T1")
T2 = TypeVar("T2")


class Pipe(AbstractEmitter, Generic[T1, T2], ABC):
    """
    Abstract Pipe defining async event-based read/write/terminate interface.
    """

    @abstractmethod
    async def write(self, chunk: T1) -> bool:
        pass

    @abstractmethod
    async def read(self, timeout: Optional[float] = None) -> Optional[T2]:
        pass

    @abstractmethod
    async def terminate(self, *args: Any) -> None:
        pass

    @staticmethod
    def create_pair() -> Tuple["Pipe[Any, Any]", "Pipe[Any, Any]"]:
        class InMemoryPipe(Pipe[Any, Any]):
            def __init__(self):
                super().__init__()
                self._closed = False
                self._buffer: asyncio.Queue[Optional[Any]] = asyncio.Queue()
                self.connected_pipe: Optional["InMemoryPipe"] = None

            async def write(self, chunk: Any) -> bool:
                if self._closed or not self.connected_pipe or self.connected_pipe._closed:
                    return False
                await self.connected_pipe._buffer.put(chunk)
                return True

            async def read(self, timeout: Optional[float] = None) -> Optional[Any]:
                if self._closed:
                    return None
                try:
                    if timeout is not None and timeout > 0:
                        return await asyncio.wait_for(self._buffer.get(), timeout)
                    else:
                        return await self._buffer.get()
                except asyncio.TimeoutError:
                    return None
                except asyncio.CancelledError:
                    # If read is cancelled, put back the None if it was a termination signal
                    if not self._buffer.empty():
                        item = self._buffer.get_nowait()
                        if item is None:
                            await self._buffer.put(None)
                    raise

            async def terminate(self, *args: Any) -> None:
                if self._closed:
                    return
                self._closed = True
                # Signal termination to any pending reads
                await self._buffer.put(None)
                await self._notify("close", *args)  # Notify external listeners

                if self.connected_pipe and not self.connected_pipe._closed:
                    await self.connected_pipe._buffer.put(None)  # Signal termination to connected pipe
                    await self.connected_pipe.terminate(*args)  # Recursively terminate connected pipe

        a = InMemoryPipe()
        b = InMemoryPipe()
        a.connected_pipe = b
        b.connected_pipe = a
        return a, b

    def make_event_based(self) -> "EventPipe[T1, T2]":
        parent = self

        class PipeToEvent(EventPipe[T1, T2]):
            def __init__(self):
                super().__init__()
                self._closed = False

                # Spawn a task to pump data from parent.read into events
                self._pump_task = asyncio.create_task(self._pump())

            async def _pump(self):
                while not self._closed:
                    chunk = await parent.read()
                    if chunk is None:
                        print("PipeToEvent: Received termination signal, terminating event pipe.")
                        await self.terminate()
                        break
                    await self._notify("data", chunk)

            async def write(self, chunk: T1) -> bool:
                return await parent.write(chunk)

            async def terminate(self, *args: Any) -> None:
                if self._closed:
                    return
                self._closed = True
                print("PipeToEvent: Terminating event pipe.")
                await parent.terminate(*args)
                self._emit("close", *args)
                if self._pump_task:
                    self._pump_task.cancel()
                    try:
                        await self._pump_task
                    except asyncio.CancelledError:
                        pass

        return PipeToEvent()
