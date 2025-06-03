import asyncio
import socket
from typing import Any, Optional, Tuple, Union
from .pipe import Pipe


class TcpDuplex(Pipe[bytes, bytes]):
    """
    A TCP duplex pipe that implements Pipe<bytes, bytes> for network communication.
    Provides both client and server functionality for TCP connections.
    """
    
    def __init__(self, reader: Optional[asyncio.StreamReader] = None, 
                 writer: Optional[asyncio.StreamWriter] = None):
        super().__init__()
        self._reader = reader
        self._writer = writer
        self._connected = False
        self._closed = False
        self._read_task: Optional[asyncio.Task] = None
        
    @classmethod
    async def create_connection(cls, host: str, port: int, 
                              timeout: Optional[float] = None) -> 'TcpDuplex':
        """
        Create a TCP client connection to the specified host and port.
        
        Args:
            host: The hostname or IP address to connect to
            port: The port number to connect to
            timeout: Optional timeout for the connection attempt
            
        Returns:
            A connected TcpDuplex instance
            
        Raises:
            ConnectionError: If the connection fails
            asyncio.TimeoutError: If the connection times out
        """
        try:
            if timeout:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=timeout
                )
            else:
                reader, writer = await asyncio.open_connection(host, port)
                
            tcp_duplex = cls(reader, writer)
            await tcp_duplex._setup_connection()
            return tcp_duplex
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}")
    
    @classmethod
    async def create_server(cls, host: str = '0.0.0.0', port: int = 0, 
                           backlog: int = 100) -> 'TcpServer':
        """
        Create a TCP server that listens for incoming connections.
        
        Args:
            host: The hostname or IP address to bind to (default: '0.0.0.0')
            port: The port number to bind to (default: 0 for auto-assignment)
            backlog: The maximum number of queued connections
            
        Returns:
            A TcpServer instance
        """
        return await TcpServer.create(host, port, backlog)
    
    @classmethod
    def from_socket(cls, sock: socket.socket) -> 'TcpDuplex':
        """
        Create a TcpDuplex from an existing socket.
        
        Args:
            sock: An existing connected socket
            
        Returns:
            A TcpDuplex instance wrapping the socket
        """
        # This will be set up when the connection is established
        tcp_duplex = cls()
        tcp_duplex._socket = sock
        return tcp_duplex
    
    async def connect(self, host: str, port: int, timeout: Optional[float] = None) -> None:
        """
        Connect to a remote TCP server.
        
        Args:
            host: The hostname or IP address to connect to
            port: The port number to connect to
            timeout: Optional timeout for the connection attempt
            
        Raises:
            ConnectionError: If already connected or connection fails
            asyncio.TimeoutError: If the connection times out
        """
        if self._connected:
            raise ConnectionError("Already connected")
            
        try:
            if timeout:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=timeout
                )
            else:
                self._reader, self._writer = await asyncio.open_connection(host, port)
                
            await self._setup_connection()
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}")
    
    async def _setup_connection(self) -> None:
        """Set up the connection and start reading data."""
        if not self._reader or not self._writer:
            raise RuntimeError("Reader or writer not initialized")
            
        self._connected = True
        self._closed = False
        
        # Start the read loop
        self._read_task = asyncio.create_task(self._read_loop())
        
        # Emit connection event
        await self._emit("connect")
    
    async def _read_loop(self) -> None:
        """Continuously read data from the TCP connection and emit data events."""
        try:
            while self._connected and not self._closed and self._reader:
                try:
                    # Read data in chunks
                    data = await self._reader.read(8192)
                    if not data:
                        # Connection closed by remote
                        break
                    
                    # Emit data event
                    await self._emit("data", data)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    await self._emit("error", e)
                    break
                    
        except Exception as e:
            await self._emit("error", e)
        finally:
            if not self._closed:
                await self._close_connection()
    
    async def write(self, chunk: bytes) -> bool:
        """
        Write data to the TCP connection.
        
        Args:
            chunk: The bytes to write
            
        Returns:
            True if the write was successful
            
        Raises:
            ConnectionError: If not connected
            TypeError: If chunk is not bytes
        """
        if not self._connected or self._closed:
            raise ConnectionError("Not connected")
            
        if not isinstance(chunk, (bytes, bytearray)):
            raise TypeError("Chunk must be bytes or bytearray")
            
        if not self._writer:
            raise ConnectionError("Writer not available")
            
        try:
            self._writer.write(chunk)
            await self._writer.drain()
            return True
            
        except Exception as e:
            await self._emit("error", e)
            return False
    
    async def terminate(self, *args: Any) -> None:
        """
        Terminate the TCP connection.
        
        Args:
            *args: Optional arguments (code, reason)
        """
        await self._close_connection(*args)
    
    async def _close_connection(self, *args: Any) -> None:
        """Close the TCP connection and clean up resources."""
        if self._closed:
            return
            
        self._closed = True
        self._connected = False
        
        # Cancel the read task
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
        
        # Close the writer
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass  # Ignore errors during cleanup
        
        # Emit close event
        await self._emit("close", *args)
    
    def is_connected(self) -> bool:
        """Check if the TCP connection is active."""
        return self._connected and not self._closed
    
    def get_peer_info(self) -> Optional[Tuple[str, int]]:
        """Get the remote peer's address and port."""
        if self._writer and self._connected:
            try:
                return self._writer.get_extra_info('peername')
            except Exception:
                pass
        return None
    
    def get_local_info(self) -> Optional[Tuple[str, int]]:
        """Get the local socket's address and port."""
        if self._writer and self._connected:
            try:
                return self._writer.get_extra_info('sockname')
            except Exception:
                pass
        return None


class TcpServer:
    """
    A TCP server that creates TcpDuplex instances for incoming connections.
    """
    
    def __init__(self, server: asyncio.Server):
        self._server = server
        self._connections: set[TcpDuplex] = set()
    
    @classmethod
    async def create(cls, host: str = '0.0.0.0', port: int = 0, 
                    backlog: int = 100) -> 'TcpServer':
        """
        Create and start a TCP server.
        
        Args:
            host: The hostname or IP address to bind to
            port: The port number to bind to (0 for auto-assignment)
            backlog: The maximum number of queued connections
            
        Returns:
            A started TcpServer instance
        """
        tcp_server = cls.__new__(cls)
        tcp_server._connections = set()
        
        async def client_connected_cb(reader: asyncio.StreamReader, 
                                    writer: asyncio.StreamWriter) -> None:
            tcp_duplex = TcpDuplex(reader, writer)
            tcp_server._connections.add(tcp_duplex)
            
            # Set up connection cleanup
            async def cleanup(*args):
                tcp_server._connections.discard(tcp_duplex)
            tcp_duplex.on("close", cleanup)
            
            await tcp_duplex._setup_connection()
            await tcp_server._emit_connection(tcp_duplex)
        
        server = await asyncio.start_server(
            client_connected_cb, host, port, backlog=backlog
        )
        
        tcp_server._server = server
        return tcp_server
    
    async def _emit_connection(self, tcp_duplex: TcpDuplex) -> None:
        """Emit a connection event. Override this method to handle new connections."""
        pass
    
    def on_connection(self, handler) -> None:
        """
        Set a handler for new connections.
        
        Args:
            handler: A function that takes a TcpDuplex as argument
        """
        original_emit = self._emit_connection
        
        async def new_emit(tcp_duplex: TcpDuplex) -> None:
            await original_emit(tcp_duplex)
            if asyncio.iscoroutinefunction(handler):
                await handler(tcp_duplex)
            else:
                handler(tcp_duplex)
        
        self._emit_connection = new_emit
    
    def get_address(self) -> Tuple[str, int]:
        """Get the server's listening address and port."""
        return self._server.sockets[0].getsockname()[:2]
    
    def get_connections(self) -> set[TcpDuplex]:
        """Get all active connections."""
        return self._connections.copy()
    
    async def close(self) -> None:
        """Close the server and all connections."""
        # Close all connections
        close_tasks = [conn.terminate() for conn in self._connections.copy()]
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Close the server
        self._server.close()
        await self._server.wait_closed()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
