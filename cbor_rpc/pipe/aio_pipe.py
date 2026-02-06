from typing import Any, Optional, TypeVar, Generic, Union
import asyncio
from abc import ABC
from .event_pipe import EventPipe  # Assuming EventPipe is in a separate module

# Constrain T1 and T2 to bytes or bytearray for type safety with asyncio streams
T1 = TypeVar("T1", bound=Union[bytes, bytearray])
T2 = TypeVar("T2", bound=Union[bytes, bytearray])


class AioPipe(EventPipe[T1, T2], ABC):
    """
    Abstract base class for asynchronous pipes that wrap asyncio.StreamReader and asyncio.StreamWriter.
    Extends EventPipe to provide event-based communication for async I/O.

    Attributes:
        DEFAULT_READ_CHUNK_SIZE (int): Default size of chunks to read from the stream (8192 bytes).
    """

    DEFAULT_READ_CHUNK_SIZE = 8192

    def __init__(
        self,
        reader: Optional[asyncio.StreamReader] = None,
        writer: Optional[asyncio.StreamWriter] = None,
        chunk_size: int = DEFAULT_READ_CHUNK_SIZE,
    ):
        """
        Initialize the AioPipe with optional reader, writer, and chunk size.

        Args:
            reader: Optional asyncio.StreamReader for reading data.
            writer: Optional asyncio.StreamWriter for writing data.
            chunk_size: Size of chunks to read from the stream (default: 8192 bytes).

        Raises:
            ValueError: If only one of reader or writer is provided.
        """
        super().__init__()
        if (reader is None) != (writer is None):  # Both must be None or both non-None
            raise ValueError("Both reader and writer must be provided or neither")
        self._reader = reader
        self._writer = writer
        self._chunk_size = chunk_size
        self._connected = False
        self._closed = False
        self._read_task: Optional[asyncio.Task] = None

    async def _setup_connection(self) -> None:
        """
        Set up the connection and start reading data.

        Raises:
            RuntimeError: If reader or writer is not initialized.
        """
        if not self._reader or not self._writer:
            raise RuntimeError("Reader or writer not initialized")

        self._connected = True
        self._closed = False
        self._read_task = asyncio.create_task(self._read_loop())

        try:
            await self._notify("connect")
        except Exception as e:
            self._emit("error", e)  # Synchronous _emit
            await self._close_connection()
            raise

    async def _read_loop(self) -> None:
        """
        Continuously read data from the connection and emit data events.

        Stops on EOF, cancellation, or error. Closes the connection in case of errors.
        """
        try:
            while self._connected and not self._closed and self._reader:
                try:
                    data = await self._reader.read(self._chunk_size)
                    if not data:  # EOF reached
                        break
                    try:
                        await self._notify("data", data)
                    except Exception as e:
                        self._emit("error", e)  # Synchronous _emit
                        break
                except asyncio.CancelledError:
                    break
                except Exception as e:  # Catch BaseException for GeneratorExit/other BaseExceptions
                    self._emit("error", e)  # Synchronous _emit
                    break
        except Exception as e:  # Catch BaseException for GeneratorExit/other BaseExceptions
            self._emit("error", e)  # Synchronous _emit
        finally:
            if not self._closed:
                await self._close_connection()

    async def _close_connection(self, *args: Any) -> None:
        """
        Close the connection and clean up resources.

        Args:
            *args: Optional arguments to pass to the 'close' event.
        """
        if self._closed:
            return

        self._closed = True
        self._connected = False

        # Cancel the read task
        if self._read_task and not self._read_task.done():
            task, self._read_task = self._read_task, None
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._emit("error", e)  # Synchronous _emit

        # Close the writer
        if self._writer:
            writer, self._writer = self._writer, None
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                self._emit("error", e)  # Synchronous _emit

        # Emit close event
        try:
            self._emit("close", *args)  # Synchronous _emit
        except Exception as e:
            print(f"AioPipe: Failed to emit close event: {e}")

    def is_connected(self) -> bool:
        """
        Check if the connection is active.

        Returns:
            bool: True if connected and not closed, False otherwise.
        """
        return self._connected and not self._closed

    async def write(self, chunk: T1) -> bool:
        """
        Write data to the connection.

        Args:
            chunk: The data to write (bytes or bytearray).

        Returns:
            bool: True if the write was successful, False otherwise.

        Raises:
            ConnectionError: If not connected or writer is unavailable.
            TypeError: If chunk is not bytes or bytearray.
        """
        if not self._connected or self._closed:
            raise ConnectionError("Not connected")
        if not self._writer:
            raise ConnectionError("Writer not available")
        if not isinstance(chunk, (bytes, bytearray)):
            raise TypeError(f"Expected bytes or bytearray, got {type(chunk).__name__}")

        try:
            self._writer.write(chunk)
            await self._writer.drain()
            return True
        except Exception as e:
            self._emit("error", e)  # Synchronoous _emit
            return False

    async def terminate(self, *args: Any) -> None:
        """
        Terminate the connection.

        Args:
            *args: Optional arguments (e.g., code, reason) to pass to the close event.
        """
        await self._close_connection(*args)
