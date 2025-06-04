from typing import Any, Callable, Optional, Set, TypeVar, Generic
from abc import ABC, abstractmethod
import asyncio
from .emitter import AbstractEmitter
from .pipe import Pipe

# Generic type variable for pipe types
P = TypeVar('P', bound=Pipe)


class Server(AbstractEmitter, Generic[P]):
    """
    Abstract server class that manages connections and emits connection events.
    
    Type parameter P specifies the type of Pipe that this server handles.
    """
    
    def __init__(self):
        super().__init__()
        self._connections: Set[P] = set()
        self._running = False

    @abstractmethod
    async def start(self, *args, **kwargs) -> Any:
        """
        Start the server.
        
        Returns:
            Server-specific information (e.g., address, port)
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the server and clean up resources."""
        pass

    async def _add_connection(self, pipe: P) -> None:
        """
        Add a new connection and emit a connection event.
        
        Args:
            pipe: The pipe representing the connection
        """
        self._connections.add(pipe)
        
        # Set up cleanup when connection closes
        async def cleanup(*args):
            self._connections.discard(pipe)
        pipe.on("close", cleanup)
        
        # Emit connection event
        await self._emit("connection", pipe)

    def get_connections(self) -> Set[P]:
        """Get all active connections."""
        return self._connections.copy()

    def is_running(self) -> bool:
        """Check if the server is running."""
        return self._running

    async def close_all_connections(self) -> None:
        """Close all active connections."""
        close_tasks = [conn.terminate() for conn in self._connections.copy()]
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

    def on_connection(self, handler: Callable[[P], None]) -> None:
        """
        Register a handler for new connections.
        
        Args:
            handler: Function that takes a Pipe of type P as argument
        """
        self.on("connection", handler)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
