from typing import Any, TypeVar, Generic, Callable
from abc import ABC, abstractmethod
import asyncio
import inspect
from .emitter import AbstractEmitter

# Generic type variables
T1 = TypeVar('T1')
T2 = TypeVar('T2')


class Pipe(AbstractEmitter, Generic[T1, T2]):
    @abstractmethod
    async def write(self, chunk: T1) -> bool:
        pass

    @abstractmethod
    async def terminate(self, *args: Any) -> None:
        pass

    def on(self, event: str, handler: Callable) -> None:
        if event == "data":
            async def data_handler(data: T2) -> None:
                if inspect.iscoroutinefunction(handler):
                    await handler(data)
                else:
                    handler(data)
            super().on(event, data_handler)
        elif event in ("error", "close"):
            async def error_handler(err: Exception) -> None:
                if inspect.iscoroutinefunction(handler):
                    await handler(err)
                else:
                    handler(err)
            super().on(event, error_handler)
        else:
            super().on(event, handler)

    def pipeline(self, event: str, handler: Callable) -> None:
        if event == "data":
            async def async_pipeline(data: T2) -> None:
                if inspect.iscoroutinefunction(handler):
                    await handler(data)
                else:
                    handler(data)
            super().pipeline(event, async_pipeline)
        else:
            super().pipeline(event, handler)

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
    def make_pipe(writer: Callable, terminator: Callable) -> 'Pipe[T1, T2]':
        class ConcretePipe(Pipe[T1, T2]):
            async def write(self, chunk: T1) -> bool:
                if inspect.iscoroutinefunction(writer):
                    await writer(chunk)
                else:
                    writer(chunk)
                await self._emit("data", chunk)  # Changed from _notify to _emit
                return True

            async def terminate(self, *args: Any) -> None:
                if inspect.iscoroutinefunction(terminator):
                    await terminator(*args)
                else:
                    terminator(*args)
                await self._emit("close", *args)
        return ConcretePipe()


class SimplePipe(Pipe[T1, T1], Generic[T1]):
    def __init__(self):
        super().__init__()

    async def write(self, chunk: T1) -> bool:
        await self._emit("data", chunk)  # Changed from _notify to _emit
        return True

    async def terminate(self, *args: Any) -> None:
        await self._emit("close", *args)


class Duplex(Pipe[T1, T2], Generic[T1, T2]):
    def __init__(self):
        super().__init__()
        self.reader: Pipe[T1, Any] = SimplePipe()
        self.writer: Pipe[Any, T2] = SimplePipe()
        
        # Set up error propagation
        async def forward_error(err):
            await self._emit("error", err)
            
        self.reader.on("error", forward_error)
        self.writer.on("error", forward_error)
        
        # Forward data events from reader to this pipe
        async def forward_data(chunk):
            await self._emit("data", chunk)
        self.reader.on("data", forward_data)

    async def write(self, chunk: Any) -> bool:
        result = await self.writer.write(chunk)
        return result

    async def terminate(self, *args: Any) -> None:
        await asyncio.gather(
            self.reader.terminate(*args), 
            self.writer.terminate(*args)
        )
        await self._emit("close", *args)
