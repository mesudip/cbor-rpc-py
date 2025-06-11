from abc import abstractmethod
from typing import Any, Generic, TypeVar, Union, overload

from cbor_rpc.pipe import EventPipe
from cbor_rpc.pipe import Pipe
from .event_transformer_pipe import EventTransformerPipe
from .base_exception import NeedsMoreDataException
from .transformer_pipe import TransformerPipe

T1 = TypeVar("T1")
T2 = TypeVar("T2")

# Sync Transformer (no async methods)
class Transformer(Generic[T1, T2]):
    def __init__(self):
        super().__init__()
        self._closed = False

    def is_closed(self) -> bool:
        return self._closed
    
    @abstractmethod
    def encode(self, data: T1) -> Any:
        pass

    @abstractmethod
    def decode(self, data: Any) -> T2:
        pass

    def wait_next_data(self):
       raise NeedsMoreDataException()
        
    @overload
    def bind(self, pipe: Pipe) -> TransformerPipe: ...
    @overload
    def bind(self, pipe: EventPipe) -> EventTransformerPipe: ...

    def applyTransformer(self, pipe: Union[Pipe, EventPipe]) -> Union[TransformerPipe, EventTransformerPipe]:
        if isinstance(pipe, EventPipe):
            return EventTransformerPipe(pipe, self.to_async())
        elif isinstance(pipe, Pipe):
            return TransformerPipe(pipe, self.to_async())
        else:
            raise TypeError("Invalid pipe type")

    def to_async(self) -> 'AsyncTransformer[T1, T2]':
        parent = self

        class WrappedAsyncTransformer(AsyncTransformer[T1, T2]):
            async def encode(self, data: T1) -> Any:
                return parent.encode(data)

            async def decode(self, data: Any) -> T2:
                return parent.decode(data)

            def is_closed(self) -> bool:
                return parent.is_closed()

        return WrappedAsyncTransformer()

# Async Transformer (async methods)
class AsyncTransformer(Generic[T1, T2]):
    def __init__(self):
        super().__init__()
        self._closed = False

    def is_closed(self) -> bool:
        return self._closed
    
    @abstractmethod
    async def encode(self, data: T1) -> Any:
        pass

    @abstractmethod
    async def decode(self, data: Any) -> T2:
        pass

    def wait_next_data(self):
       raise NeedsMoreDataException()

    @overload
    def bind(self, pipe: Pipe) -> TransformerPipe: ...
    @overload
    def bind(self, pipe: EventPipe) -> EventTransformerPipe: ...

    def applyTransformer(self, pipe: Union[Pipe, EventPipe]) -> Union[TransformerPipe, EventTransformerPipe]:
        if isinstance(pipe, EventPipe):
            return EventTransformerPipe(pipe)
        elif isinstance(pipe, Pipe):
            return TransformerPipe(pipe)
        else:
            raise TypeError("Invalid pipe type")
