from typing import Any, TypeVar, Generic, Callable, Tuple
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

    @staticmethod
    def create_pair() -> Tuple['Pipe[Any, Any]', 'Pipe[Any, Any]']:
        """
        Create a pair of connected pipes for bidirectional communication.
        
        Returns:
            A tuple of (pipe1, pipe2) where data written to pipe1 is emitted on pipe2 and vice versa.
        """
        pipe1 = SimplePipe()
        pipe2 = SimplePipe()
        Pipe.attach(pipe1, pipe2)
        return pipe1, pipe2


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


class Duplex(Pipe[T1, T2], Generic[T1, T2]):
    def __init__(self):
        super().__init__()
        self.reader: Pipe[T1, Any] = SimplePipe()
        self.writer: Pipe[Any, T2] = SimplePipe()
        self._closed = False
        
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
        if self._closed:
            return False
        result = await self.writer.write(chunk)
        return result

    async def terminate(self, *args: Any) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.gather(
            self.reader.terminate(*args), 
            self.writer.terminate(*args)
        )
        await self._emit("close", *args)


class Transformer(Pipe[T1, T2], Generic[T1, T2]):
    """
    Abstract transformer pipe that encodes data when writing and decodes data when emitting events.
    """
    
    def __init__(self, underlying_pipe: Pipe[Any, Any]):
        super().__init__()
        self.underlying_pipe = underlying_pipe
        self._closed = False
        
        # Forward events from underlying pipe, but decode data events
        async def on_underlying_data(data: Any):
            try:
                decoded_data = await self.decode(data)
                await self._emit("data", decoded_data)
            except Exception as e:
                await self._emit("error", e)
        
        async def on_underlying_close(*args):
            await self._emit("close", *args)
        
        async def on_underlying_error(error):
            await self._emit("error", error)
        
        self.underlying_pipe.on("data", on_underlying_data)
        self.underlying_pipe.on("close", on_underlying_close)
        self.underlying_pipe.on("error", on_underlying_error)

    async def write(self, chunk: T1) -> bool:
        """Write data after encoding it."""
        if self._closed:
            return False
        try:
            encoded_chunk = await self.encode(chunk)
            return await self.underlying_pipe.write(encoded_chunk)
        except Exception as e:
            await self._emit("error", e)
            return False

    async def terminate(self, *args: Any) -> None:
        """Terminate the underlying pipe."""
        if self._closed:
            return
        self._closed = True
        await self.underlying_pipe.terminate(*args)

    @abstractmethod
    async def encode(self, data: T1) -> Any:
        """
        Encode data before writing to the underlying pipe.
        
        Args:
            data: The data to encode
            
        Returns:
            The encoded data
        """
        pass

    @abstractmethod
    async def decode(self, data: Any) -> T2:
        """
        Decode data received from the underlying pipe.
        
        Args:
            data: The data to decode
            
        Returns:
            The decoded data
        """
        pass

    @staticmethod
    def create_pair(encoder1: Callable, decoder1: Callable, 
                   encoder2: Callable, decoder2: Callable) -> Tuple['Transformer', 'Transformer']:
        """
        Create a pair of connected transformer pipes.
        
        Args:
            encoder1: Encoder function for the first transformer
            decoder1: Decoder function for the first transformer
            encoder2: Encoder function for the second transformer
            decoder2: Decoder function for the second transformer
            
        Returns:
            A tuple of (transformer1, transformer2)
        """
        pipe1, pipe2 = Pipe.create_pair()
        
        class ConcreteTransformer1(Transformer):
            async def encode(self, data):
                if asyncio.iscoroutinefunction(encoder1):
                    return await encoder1(data)
                return encoder1(data)
            
            async def decode(self, data):
                if asyncio.iscoroutinefunction(decoder1):
                    return await decoder1(data)
                return decoder1(data)
        
        class ConcreteTransformer2(Transformer):
            async def encode(self, data):
                if asyncio.iscoroutinefunction(encoder2):
                    return await encoder2(data)
                return encoder2(data)
            
            async def decode(self, data):
                if asyncio.iscoroutinefunction(decoder2):
                    return await decoder2(data)
                return decoder2(data)
        
        transformer1 = ConcreteTransformer1(pipe1)
        transformer2 = ConcreteTransformer2(pipe2)
        
        return transformer1, transformer2
