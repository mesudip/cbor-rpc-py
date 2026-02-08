import asyncio
import asyncssh
from typing import Optional, List, Union

from cbor_rpc.pipe.aio_pipe import AioPipe


class SshPipe(AioPipe[bytes, bytes]):
    """
    A Pipe implementation that works over an SSH connection's channel (process).
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        ssh_channel: asyncssh.SSHClientChannel|asyncssh.SSHClientProcess,
    ):
        super().__init__(reader, writer)

        self._stderr_task = asyncio.create_task(self._stderr_loop())
        self._ssh_channel = ssh_channel
        
    async def _stderr_loop(self) -> None:
        """
        Continuously read data from the connection and emit data events.

        Stops on EOF, cancellation, or error. Closes the connection in case of errors.
        """
        try:
            while self._connected and not self._closed:
                try:
                    data = await self._ssh_channel.stderr.read(self._chunk_size)
                    if not data:  # EOF reached
                        break
                    try:
                        await self._notify("data", data)
                    except Exception as e:
                        self._emit("error", e)  # Synchronous _emit
                        break
                except asyncio.CancelledError:
                    break
                except BaseException as e:
                    if isinstance(e, GeneratorExit):
                        break
                    self._emit("error", e)  # Synchronous _emit
                    break
        except BaseException as e:
            if not isinstance(e, (asyncio.CancelledError, GeneratorExit)):
                self._emit("error", e)  # Synchronous _emit
        finally:
            if not self._stderr_task.cancelled():
                self._stderr_task.cancel()
    async def terminate(self) -> None:
        """
        Closes the SSH channel.
        """
        # Close the SSH channel
        if self._ssh_channel and not self._ssh_channel.is_closing():
            await super().write_eof()  # Signal EOF to the remote process
            print("Sent EOF to SSH channel, waiting for it to close...")

            print("Terminating SSH channel...")
            self._ssh_channel.terminate()
            try:
                # Wait "for" graceful closure for a short time
                print("waiting")
                await asyncio.wait_for(self._ssh_channel.wait_closed(), timeout=4.0)
            except asyncio.TimeoutError:
                # If timed out, try to force terminate

                    if isinstance(self._ssh_channel, asyncssh.SSHClientChannel):
                        cl_chan: asyncssh.SSHClientChannel = self._ssh_channel
                        print("Force aborting SSH channel...")
                        cl_chan.abort()
                    elif isinstance(self._ssh_channel, asyncssh.SSHClientProcess):
                        print("Force killing SSH process...")
                        cl_proc: asyncssh.SSHClientProcess = self._ssh_channel
                        cl_proc.kill()    
            await self._ssh_channel.wait_closed()  # Ensure the AioPipe is fully closed
    async def wait_closed(self) -> None:
        await self._ssh_channel.wait_closed()
        if self._ssh_channel:
            await self._ssh_channel.wait_closed()


class SshServer:
    """
    Manages an SSH connection and allows spawning multiple SshPipes or Unix connections.
    """
    def __init__(
        self, 
        connection: asyncssh.SSHClientConnection,
        host: str,
        port: int = 22,
    ):
        self._connection = connection
        self.host = host
        self.port = port

    @classmethod
    async def connect(
        cls, 
        host: str,
        port: int = 22,
        username: Optional[str] = None,
        password: Optional[str] = None,
        known_hosts: Optional[Union[str, List[str]]] = None,
        client_keys: Optional[list] = None,
        ssh_key_content: Optional[str] = None,
        ssh_key_passphrase: Optional[str] = None,
        **connect_kwargs
    ) -> "SshServer":
        
        keys = client_keys
        if ssh_key_content:
            key = asyncssh.import_private_key(ssh_key_content, passphrase=ssh_key_passphrase)
            if keys:
                keys.append(key)
            else:
                keys = [key]
        
        connection = await asyncssh.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            known_hosts=known_hosts,
            client_keys=keys,
            **connect_kwargs,
        )
        return cls(connection, host=host, port=port)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def run_command(self, command: str = "sh -l") -> SshPipe:
        """
        Runs a command on the remote host and returns an SshPipe attached to it.
        """
        if not self._connection:
            raise RuntimeError("SshServer is not connected. Call connect() first.")
            
        channel = await self._connection.create_process(command, term_type=None, encoding=None, stdin=asyncssh.PIPE)

        reader = channel.stdout
        writer = channel.stdin

        pipe = SshPipe(reader, writer, channel)
        # We need to setup connection for the AioPipe
        await pipe._setup_connection()
        return pipe
    @staticmethod
    async def connect_and_run(args="..."):
        # TODO
        pass
    async def open_unix_connection(self, path: str) -> AioPipe:
        """
        Opens a direct stream connection to a Unix socket on the remote host.
        Returns an AioPipe wrapping the connection.
        """
        if not self._connection:
            raise RuntimeError("SshServer is not connected. Call connect() first.")

        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(loop=loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=loop)

        channel, _ = await self._connection.create_unix_connection(lambda: protocol, path)
        writer = asyncio.StreamWriter(channel, protocol, reader, loop)

        pipe = AioPipe(reader, writer)
        await pipe._setup_connection()
        return pipe

    async def close(self):
        if self._connection:
            self._connection.close()
            await self._connection.wait_closed()
            self._connection = None
