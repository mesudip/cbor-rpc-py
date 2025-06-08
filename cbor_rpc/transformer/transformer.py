from typing import Any, TypeVar, Generic, Callable, Tuple, Optional
from abc import ABC, abstractmethod
import asyncio
import inspect
from cbor_rpc.async_pipe import Pipe
from cbor_rpc.sync_pipe import SyncPipe
import queue
import threading
from typing import Union

# Generic type variables
T1 = TypeVar('T1')
T2 = TypeVar('T2')

class Transformer(Generic[T1, T2]):
    """
    Abstract transformer that can wrap both sync and async pipes.
    Encodes data when writing and decodes data when reading/emitting events.
    """
    
    def __init__(self, underlying_pipe: Union[Pipe[Any, Any], SyncPipe[Any, Any]]):
        self.underlying_pipe = underlying_pipe
        self._closed = False
        self._is_sync_pipe = isinstance(underlying_pipe, SyncPipe)
        
        if not self._is_sync_pipe:
            # For async pipes, set up event handlers
            super().__init__()
            
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
    
    # Async methods for async pipes
    async def write(self, chunk: T1) -> bool:
        """Write data after encoding it (async version)."""
        if self._closed:
            return False
        
        if self._is_sync_pipe:
            raise RuntimeError("Use write_sync() for SyncPipe")
        
        try:
            encoded_chunk = await self.encode(chunk)
            return await self.underlying_pipe.write(encoded_chunk)
        except Exception as e:
            await self._emit("error", e)
            return False

    async def terminate(self, *args: Any) -> None:
        """Terminate the underlying pipe (async version)."""
        if self._closed:
            return
        self._closed = True
        
        if not self._is_sync_pipe:
            await self.underlying_pipe.terminate(*args)
        else:
            self.underlying_pipe.close()
    
    # Sync methods for sync pipes
    def write_sync(self, chunk: T1) -> bool:
        """Write data after encoding it (sync version)."""
        if self._closed:
            return False
        
        if not self._is_sync_pipe:
            raise RuntimeError("Use write() for async Pipe")
        
        try:
            encoded_chunk = self.encode_sync(chunk)
            return self.underlying_pipe.write(encoded_chunk)
        except Exception as e:
            return False
    
    def read_sync(self, timeout: Optional[float] = None) -> Optional[T2]:
        """Read and decode data (sync version)."""
        if self._closed:
            return None
        
        if not self._is_sync_pipe:
            raise RuntimeError("Use event handlers for async Pipe")
        
        try:
            raw_data = self.underlying_pipe.read(timeout)
            if raw_data is None:
                return None
            return self.decode_sync(raw_data)
        except Exception as e:
            return None
    
    def close_sync(self) -> None:
        """Close the transformer (sync version)."""
        if self._closed:
            return
        self._closed = True
        
        if self._is_sync_pipe:
            self.underlying_pipe.close()
    
    def is_sync_pipe(self) -> bool:
        """Check if this transformer wraps a sync pipe."""
        return self._is_sync_pipe
    
    # Abstract methods - async versions
    @abstractmethod
    async def encode(self, data: T1) -> Any:
        """Encode data before writing to the underlying pipe (async version)."""
        pass

    @abstractmethod
    async def decode(self, data: Any) -> T2:
        """Decode data received from the underlying pipe (async version)."""
        pass
    
    # Abstract methods - sync versions (with default implementations that call async versions)
    def encode_sync(self, data: T1) -> Any:
        """Encode data before writing to the underlying pipe (sync version)."""
        # Default implementation for backwards compatibility
        # Subclasses should override this for true sync operation
        if asyncio.iscoroutinefunction(self.encode):
            raise NotImplementedError("Sync encoding not implemented for this transformer")
        return asyncio.run(self.encode(data))
    
    def decode_sync(self, data: Any) -> T2:
        """Decode data received from the underlying pipe (sync version)."""
        # Default implementation for backwards compatibility
        # Subclasses should override this for true sync operation
        if asyncio.iscoroutinefunction(self.decode):
            raise NotImplementedError("Sync decoding not implemented for this transformer")
        return asyncio.run(self.decode(data))

    @staticmethod
    def create_pair(encoder1: Callable, decoder1: Callable, 
                   encoder2: Callable, decoder2: Callable,
                   use_sync: bool = False) -> Tuple['Transformer', 'Transformer']:
        """
        Create a pair of connected transformer pipes.
        
        Args:
            encoder1: Encoder function for the first transformer
            decoder1: Decoder function for the first transformer
            encoder2: Encoder function for the second transformer
            decoder2: Decoder function for the second transformer
            use_sync: If True, use SyncPipe; if False, use async Pipe
            
        Returns:
            A tuple of (transformer1, transformer2)
        """
        if use_sync:
            pipe1, pipe2 = SyncPipe.create_pair()
        else:
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
            
            def encode_sync(self, data):
                if asyncio.iscoroutinefunction(encoder1):
                    raise NotImplementedError("Encoder is async, cannot use sync method")
                return encoder1(data)
            
            def decode_sync(self, data):
                if asyncio.iscoroutinefunction(decoder1):
                    raise NotImplementedError("Decoder is async, cannot use sync method")
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
            
            def encode_sync(self, data):
                if asyncio.iscoroutinefunction(encoder2):
                    raise NotImplementedError("Encoder is async, cannot use sync method")
                return encoder2(data)
            
            def decode_sync(self, data):
                if asyncio.iscoroutinefunction(decoder2):
                    raise NotImplementedError("Decoder is async, cannot use sync method")
                return decoder2(data)
        
        transformer1 = ConcreteTransformer1(pipe1)
        transformer2 = ConcreteTransformer2(pipe2)
        
        return transformer1, transformer2
