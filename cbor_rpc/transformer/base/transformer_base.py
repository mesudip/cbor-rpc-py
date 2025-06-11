from abc import abstractmethod
from typing import Any, Generic, TypeVar, Union, overload

from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.pipe.pipe import Pipe
from cbor_rpc.transformer.base.async_transformer import EventTransformerPipe
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException
from cbor_rpc.transformer.base.sync_transformer import TransformerPipe

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

    def applyTransformer(self, pipe: Union[Pipe, EventPipe], transformer: 'Transformer') -> Union[TransformerPipe, EventTransformerPipe]:
        if isinstance(pipe, EventPipe):
            return EventTransformerPipe(pipe, transformer)
        elif isinstance(pipe, Pipe):
            return TransformerPipe(pipe, transformer)
        else:
            raise TypeError("Invalid pipe type")


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

    def applyTransformer(self, pipe: Union[Pipe, EventPipe], transformer: 'AsyncTransformer') -> Union[TransformerPipe, EventTransformerPipe]:
        if isinstance(pipe, EventPipe):
            return EventTransformerPipe(pipe, transformer)
        elif isinstance(pipe, Pipe):
            return TransformerPipe(pipe, transformer)
        else:
            raise TypeError("Invalid pipe type")
