from typing import Any, TypeVar, Generic, Callable, Tuple, Optional
from abc import ABC, abstractmethod
import asyncio
import inspect
from ..event.emitter import AbstractEmitter

# Generic type variables
T1 = TypeVar('T1')
T2 = TypeVar('T2')


class EventPipe(AbstractEmitter, Generic[T1, T2]):
    """
    Event Pipe or are event based way for read/write.
    You cannot directly read from a Pipe. You have to use a pipeline("data") to register one or more functions to read data.
    """
    @abstractmethod
    async def write(self, chunk: T1) -> bool:
        pass

    @abstractmethod
    async def terminate(self, *args: Any) -> None:
        pass

    @staticmethod
    def create_pair() -> Tuple['EventPipe[Any, Any]', 'EventPipe[Any, Any]']:
        """
        Create a pair of connected pipes for bidirectional communication.

        Returns:
            A tuple of (pipe1, pipe2) where data written to pipe1 is emitted on pipe2 and vice versa.
        """
        class ConnectedPipe(EventPipe[Any, Any]):
            def __init__(self):
                super().__init__()
                self.connected_pipe: Optional['ConnectedPipe'] = None
                self._closed = False

            def connect_to(self, other: 'ConnectedPipe'):
                self.connected_pipe = other
                other.connected_pipe = self

            async def write(self, chunk: Any) -> bool:
                if self._closed or not self.connected_pipe or self.connected_pipe._closed:
                    return False
                await self.connected_pipe._notify("data", chunk)
                return True

            async def terminate(self, *args: Any) -> None:
                if self._closed:
                    return
                self._closed = True
                self._emit("close", *args)
                if self.connected_pipe and not self.connected_pipe._closed:
                    self.connected_pipe._emit("close", *args)

        pipe1 = ConnectedPipe()
        pipe2 = ConnectedPipe()
        pipe1.connect_to(pipe2)

        return pipe1, pipe2
