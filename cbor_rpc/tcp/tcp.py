from abc import abstractmethod
import asyncio
import socket
from typing import Any, Callable, Optional, Tuple, Union
from cbor_rpc.pipe.aio_pipe import AioPipe
from cbor_rpc.rpc.server_base import Server


class TcpPipe(AioPipe[bytes, bytes]):
    """
    A TCP duplex pipe that implements Pipe<bytes, bytes> for network communication.
    Provides both client and server functionality for TCP connections.
    """

    def __init__(
        self,
        reader: Optional[asyncio.StreamReader] = None,
        writer: Optional[asyncio.StreamWriter] = None,
    ):
        super().__init__(reader, writer)

    @classmethod
    async def create_connection(cls, host: str, port: int, timeout: Optional[float] = None) -> "TcpPipe":
        """
        Create a TCP client connection to the specified host and port.

        Args:
            host: The hostname or IP address to connect to
            port: The port number to connect to
            timeout: Optional timeout for the connection attempt

        Returns:
            A connected TcpPipe instance

        Raises:
            ConnectionError: If the connection fails
            asyncio.TimeoutError: If the connection times out
        """
        try:
            if timeout:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
            else:
                reader, writer = await asyncio.open_connection(host, port)

            tcp_duplex = cls(reader, writer)
            await tcp_duplex._setup_connection()
            return tcp_duplex

        except Exception as e:
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}")

    @classmethod
    async def create_server(cls, host: str = "0.0.0.0", port: int = 0, backlog: int = 100) -> "TcpServer":
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

    @staticmethod
    async def create_inmemory_pair() -> Tuple["TcpPipe", "TcpPipe"]:
        """
        Create a pair of connected TCP pipes using a local server.

        Returns:
            A tuple of (client_pipe, server_pipe) connected via TCP
        """

        class SimpleTcpServer(TcpServer):
            async def accept(self, pipe: TcpPipe) -> bool:
                return True

        # Create a temporary server
        server = await SimpleTcpServer.create("127.0.0.1", 0)
        host, port = server.get_address()

        # Set up to capture the server-side connection
        server_pipe = None
        connection_ready = asyncio.Event()

        async def on_connection(pipe: TcpPipe):
            nonlocal server_pipe
            server_pipe = pipe
            connection_ready.set()

        server.on_connection(on_connection)

        try:
            # Create client connection
            client_pipe = await TcpPipe.create_connection(host, port)

            # Wait for server connection
            await connection_ready.wait()

            # Stop accepting new connections but keep the active pipes
            await server.shutdown()

            return client_pipe, server_pipe

        except Exception:
            await server.stop()
            raise

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
                    asyncio.open_connection(host, port), timeout=timeout
                )
            else:
                self._reader, self._writer = await asyncio.open_connection(host, port)

            await self._setup_connection()

        except Exception as e:
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}")

    def get_peer_info(self) -> Optional[Tuple[str, int]]:
        """Get the remote peer's address and port."""
        if self._writer and self._connected:
            try:
                return self._writer.get_extra_info("peername")
            except Exception:
                pass
        return None

    def get_local_info(self) -> Optional[Tuple[str, int]]:
        """Get the local socket's address and port."""
        if self._writer and self._connected:
            try:
                return self._writer.get_extra_info("sockname")
            except Exception:
                pass
        return None

    def get_peer_info(self) -> Optional[Tuple[str, int]]:
        """Get the remote peer's address and port."""
        if self._writer and self._connected:
            try:
                return self._writer.get_extra_info("peername")
            except Exception:
                pass
        return None

    def get_local_info(self) -> Optional[Tuple[str, int]]:
        """Get the local socket's address and port."""
        if self._writer and self._connected:
            try:
                return self._writer.get_extra_info("sockname")
            except Exception:
                pass
        return None


class TcpServer(Server[TcpPipe]):
    """
    A TCP server that creates TcpPipe instances for incoming connections.
    Extends Server[TcpPipe] to provide type-safe TCP-specific functionality.
    """

    def __init__(self, server: asyncio.Server):
        super().__init__()
        self._server = server

    @classmethod
    async def create(cls, host: str = "0.0.0.0", port: int = 0, backlog: int = 100) -> "TcpServer":
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
        Server.__init__(tcp_server)

        async def client_connected_cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            tcp_pipe = TcpPipe(reader, writer)
            await tcp_pipe._setup_connection()
            await tcp_server._add_connection(tcp_pipe)

        server = await asyncio.start_server(client_connected_cb, host, port, backlog=backlog)

        tcp_server._server = server
        tcp_server._running = True
        return tcp_server

    async def start(self, host: str = "0.0.0.0", port: int = 0, backlog: int = 100) -> Tuple[str, int]:
        """
        Start the TCP server (if not already started).

        Args:
            host: The hostname or IP address to bind to
            port: The port number to bind to
            backlog: The maximum number of queued connections

        Returns:
            A tuple of (host, port) where the server is listening
        """
        if self._running:
            return self.get_address()

        # This method is for compatibility; typically create() is used instead
        new_server = await TcpServer.create(host, port, backlog)
        self._server = new_server._server
        self._running = True
        return self.get_address()

    async def stop(self) -> None:
        """Stop the TCP server and close all connections."""
        if not self._running:
            return

        self._running = False

        # Close all connections
        await self.close_all_connections()

        # Close the server
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def shutdown(self) -> None:
        """Stop accepting new connections while keeping active connections open."""
        if not self._server:
            return

        self._running = False
        self._server.close()
        await self._server.wait_closed()

    def get_address(self) -> Tuple[str, int]:
        """Get the server's listening address and port."""
        if self._server and self._server.sockets:
            return self._server.sockets[0].getsockname()[:2]
        return ("", 0)
