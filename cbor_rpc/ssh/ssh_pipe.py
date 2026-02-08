import asyncio
import asyncssh
from typing import Optional, Tuple, Union

from cbor_rpc.pipe.aio_pipe import AioPipe


class SshPipe(AioPipe[bytes, bytes]):
    """
    A Pipe implementation that works over an SSH connection.
    It uses asyncssh to establish and manage the SSH session and channels.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        ssh_client: asyncssh.SSHClientConnection,
        ssh_channel: asyncssh.SSHClientChannel,
    ):
        super().__init__(reader, writer)
        self._ssh_client = ssh_client
        self._ssh_channel = ssh_channel

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int = 22,
        username: str = "root",
        password: Optional[str] = None,
        ssh_key_content: Optional[str] = None,
        ssh_key_passphrase: Optional[str] = None,
        known_hosts: Optional[Union[str, list]] = None,
        timeout: Optional[float] = None,
        command: str = "sh -l",
    ) -> "SshPipe":
        """
        Establishes an SSH connection and opens a session channel, returning an SshPipe.

        Args:
            host: The hostname or IP address of the SSH server.
            port: The port number for the SSH connection (default: 22).
            username: The username for SSH authentication.
            password: The password for password-based authentication (optional).
            ssh_key_content: The content of an SSH private key for key-based authentication (optional).
            known_hosts: Path to a known_hosts file or a list of host keys (optional).
                         If None, host key checking is disabled (use with caution).
            timeout: Optional timeout for the connection attempt.
            command: The command to execute on the remote host to establish the pipe (default: 'sh -l').

        Returns:
            An SshPipe instance connected over SSH.

        Raises:
            asyncssh.Error: If the SSH connection or channel establishment fails.
            asyncio.TimeoutError: If the connection times out.
        """
        client_keys = None
        if ssh_key_content:
            client_keys = [asyncssh.import_private_key(ssh_key_content, passphrase=ssh_key_passphrase)]

        try:
            conn = await asyncio.wait_for(
                asyncssh.connect(
                    host,
                    port=port,
                    options=asyncssh.SSHClientConnectionOptions(
                        username=username,
                        password=password,
                        client_keys=client_keys,
                        passphrase=ssh_key_passphrase,  # Passphrase for encrypted client keys
                        ignore_encrypted=False,  # Do not ignore encrypted keys
                    ),
                    known_hosts=known_hosts,
                ),
                timeout=timeout,
            )

            # Create a process on the SSH connection to get stdin/stdout streams.
            # Use the provided 'command' argument.
            # Explicitly set encoding=None to ensure raw bytes are handled.
            # Set term_type=None and stdin=asyncssh.PIPE to ensure stdin is always a writable stream.
            channel = await conn.create_process(command, term_type=None, encoding=None, stdin=asyncssh.PIPE)

            reader = channel.stdout
            writer = channel.stdin

            pipe = cls(reader, writer, conn, channel)
            await pipe._setup_connection()
            return pipe

        except asyncssh.Error as e:
            # asyncssh.Error expects a 'reason' argument
            raise asyncssh.Error(str(e), reason=str(e))
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"SSH connection to {host}:{port} timed out.")

    async def terminate(self) -> None:
        """
        Closes the SSH channel and the underlying SSH connection.
        """
        # Close the SSH channel and client connection
        if self._ssh_channel and not self._ssh_channel.is_closing():
            self._ssh_channel.close()
            await self._ssh_channel.wait_closed()
        if self._ssh_client and not self._ssh_client.is_closed():
            self._ssh_client.close()
            await self._ssh_client.wait_closed()

        # Terminate the AioPipe, which handles reader/writer tasks
        await super().terminate()

    async def write_eof(self) -> None:
        """
        Signals the end of the write stream to the remote process.
        For SSH, this means closing the stdin channel.
        """
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
