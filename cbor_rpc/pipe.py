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
