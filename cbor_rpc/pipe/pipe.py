from typing import Any, TypeVar, Generic, Callable, Tuple, Optional
from abc import ABC, abstractmethod
import asyncio
import inspect
from ..event.emitter import AbstractEmitter
import queue
import threading
from typing import Union

# Generic type variables
T1 = TypeVar('T1')
T2 = TypeVar('T2')



class Pipe(Generic[T1, T2]):
    """
    Synchronous pipe uses read/write methods instead of events.
    Explicit read/write is used for writing protocols that have multiple steps.
    """

    def __init__(self):
        self._closed = False
        self._queue: queue.Queue = queue.Queue()
        self._sent_data: queue.Queue = queue.Queue()  # Track data sent to other pipe for size reporting
        self._connected_pipe: Optional['Pipe'] = None

    def read(self, timeout: Optional[float] = None) -> Optional[T2]:
        """
        Read data from the pipe.

        Args:
            timeout: Maximum time to wait for data (None = block indefinitely)

        Returns:
            Data from the pipe or None if timeout/closed

        Raises:
            Exception: If pipe is closed
        """
        if self._closed:
            raise Exception("Pipe is closed")

        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def write(self, chunk: T1) -> bool:
        """
        Write data to the pipe.

        Args:
            chunk: Data to write

        Returns:
            True if successful, False if pipe is closed
        """
        if self._closed:
            return False

        # Forward to connected pipe only (not to our own queue)
        try:
            if self._connected_pipe and not self._connected_pipe._closed:
                self._sent_data.put(chunk)  # Track for size reporting
                self._connected_pipe._queue.put(chunk)
                return True
        except:
            return False

        return True
    
    def close(self) -> None:
        """Close the pipe."""
        if self._closed:
            return
        
        self._closed = True
        
        # Notify connected pipe
        if self._connected_pipe and not self._connected_pipe._closed:
            self._connected_pipe.close()
    
    def is_closed(self) -> bool:
        """Check if the pipe is closed."""
        return self._closed
    
    def available(self) -> int:
        """Get number of items available to read."""
        if self._closed:
            return 0

        # Return combined size of local queue and sent data (data we wrote that hasn't been read by the other pipe)
        local_size = self._queue.qsize()

        # If we have a connected pipe, check how much of our sent data has already been read
        if self._connected_pipe:
            # Get all items in _sent_data queue
            sent_items = list(self._sent_data.queue)

            # Remove items that are still in the other pipe's queue
            for item in list(sent_items):
                try:
                    # Try to peek at what's in the connected pipe's queue
                    # We need a copy of the other pipe's queue to avoid modifying it during iteration
                    connected_queue = list(self._connected_pipe._queue.queue)
                    if item in connected_queue:
                        # Item hasn't been read yet, so count it
                        continue
                    else:
                        # Item has been read, remove from our sent tracking
                        self._sent_data.get()
                except:
                    # If we can't access the other pipe's queue or there's an error,
                    # just use all items in _sent_data as a fallback
                    pass

            # Return total of local data and unread sent data
            return local_size + len(list(self._sent_data.queue))

        # No connected pipe, just return local size
        return local_size
    
    @staticmethod
    def create_pair() -> Tuple['Pipe[Any, Any]', 'Pipe[Any, Any]']:
        """
        Create a pair of connected sync pipes for bidirectional communication.
        
        Returns:
            A tuple of (pipe1, pipe2) where data written to pipe1 can be read from pipe2 and vice versa.
        """
        pipe1 = Pipe[Any, Any]()
        pipe2 = Pipe[Any, Any]()
        
        # Connect them bidirectionally
        pipe1._connected_pipe = pipe2
        pipe2._connected_pipe = pipe1
        
        return pipe1, pipe2

