from typing import Any, TypeVar, Generic, Callable, Tuple, Optional
from abc import ABC, abstractmethod
import asyncio
import inspect
from .emitter import AbstractEmitter
import queue
import threading
from typing import Union

# Generic type variables
T1 = TypeVar('T1')
T2 = TypeVar('T2')


class Pipe(AbstractEmitter, Generic[T1, T2]):
    """
    Async Pipe or simply Pipe are event based way for read/write. 
    You cannot directly read from a Pipe. You have to use a on("data") handler registration.
    """
    @abstractmethod
    async def write(self, chunk: T1) -> bool:
        pass

    @abstractmethod
    async def terminate(self, *args: Any) -> None:
        pass

    @staticmethod
    def attach(source: 'Pipe[Any, Any]', destination: 'Pipe[Any, Any]') -> None:
        async def source_to_destination(chunk: Any):
            await destination.write(chunk)
        
        async def destination_to_source(chunk: Any):
            await source.write(chunk)
        
        async def close_handler(*args: Any):
            await destination._emit("close", *args)
        
        source.on("data", source_to_destination)
        destination.on("data", destination_to_source)
        source.on("close", close_handler)

    @staticmethod
    def create_pair() -> Tuple['Pipe[Any, Any]', 'Pipe[Any, Any]']:
        """
        Create a pair of connected pipes for bidirectional communication.

        Returns:
            A tuple of (pipe1, pipe2) where data written to pipe1 is emitted on pipe2 and vice versa.
        """
        class ConnectedPipe(Pipe[Any, Any]):
            def __init__(self):
                super().__init__()
                self.connected_pipe: Optional['ConnectedPipe'] = None
                self._closed = False

            def connect_to(self, other: 'ConnectedPipe'):
                self.connected_pipe = other
                other.connected_pipe = self

            async def write(self, chunk: Any) -> bool:
                if self._closed:
                    return False

                # Forward to connected pipe
                if self.connected_pipe and not self.connected_pipe._closed:
                    await self.connected_pipe._emit("data", chunk)

                return True

            async def terminate(self, *args: Any) -> None:
                if self._closed:
                    return

                self._closed = True
                await self._emit("close", *args)

                # Notify connected pipe
                if self.connected_pipe and not self.connected_pipe._closed:
                    await self.connected_pipe._emit("close", *args)

            async def read(self, timeout: Optional[float] = None) -> Optional[Any]:
                """Read data from the pipe with a timeout.

                Args:
                    timeout: Maximum time to wait for data (None = no timeout)

                Returns:
                    Data from the pipe or None if timeout/closed
                """
                if self._closed:
                    return None

                # Wait for data event or timeout
                read_future = asyncio.Future()

                def handle_data(chunk: Any):
                    read_future.set_result(chunk)
                    self.off("data", handle_data)

                self.on("data", handle_data)

                try:
                    if timeout is not None and timeout > 0:
                        await asyncio.wait_for(read_future, timeout)
                    else:
                        # Wait indefinitely for data
                        await read_future
                    return read_future.result()
                except asyncio.TimeoutError:
                    return None

        pipe1 = ConnectedPipe()
        pipe2 = ConnectedPipe()
        pipe1.connect_to(pipe2)

        return pipe1, pipe2

