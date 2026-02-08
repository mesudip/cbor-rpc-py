import asyncio
import os
import tempfile
import sys
import pytest
import asyncssh
from cbor_rpc import RpcV1
from cbor_rpc.pipe.aio_pipe import AioPipe
from cbor_rpc.ssh.ssh_pipe import SshServer
from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer
from tests.integration.rpc_test_helpers import run_rpc_all_methods


async def _wait_for_remote_socket(ssh_server: SshServer, socket_path: str, server_proc, timeout: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if server_proc.exit_status is not None:
            stdout = await server_proc.stdout.read()
            stderr = await server_proc.stderr.read()
            raise RuntimeError(
                "RPC server exited before creating socket. "
                f"exit_status={server_proc.exit_status}, stdout={stdout!r}, stderr={stderr!r}"
            )

        result = await ssh_server._connection.run(f"test -S {socket_path}", check=False)
        if result.exit_status == 0:
            return

        await asyncio.sleep(0.5)

    raise RuntimeError(f"Timed out waiting for remote socket: {socket_path}")

@pytest.fixture
async def unix_socket_pipe():
    import uuid
    from pathlib import Path
    socket_path = Path(tempfile.gettempdir()) / f"cbor_rpc_test_{uuid.uuid4().hex[:8]}.sock"
    if socket_path.exists():
        socket_path.unlink()
    # Path to the actual server script
    server_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "sshd-python", "rpc_server.py"))
    # Start the server process
    proc = await asyncio.create_subprocess_exec(
        sys.executable, server_script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    await asyncio.sleep(2)
    # The server script uses /tmp/cbor-rpc.sock as the socket path
    socket_path = "/tmp/cbor-rpc.sock"
    await asyncio.sleep(2)
    try:
        reader, writer = await asyncio.open_unix_connection(socket_path)
        pipe = AioPipe(reader, writer)
        await pipe._setup_connection()
        transformer = CborStreamTransformer().apply_transformer(pipe)
        yield transformer
    finally:
        if proc.returncode is None:
            proc.terminate()
            try:
                await proc.wait()
            except:
                pass
        if os.path.exists(socket_path):
            try:
                os.remove(socket_path)
            except OSError:
                pass

@pytest.fixture
async def ssh_unix_socket_pipe(ssh_server: SshServer):
    # Start the server process inside the container
    server_cmd = "python3 /app/rpc_server.py"
    socket_path = "/tmp/cbor-rpc.sock"
    server_proc = await ssh_server._connection.create_process(server_cmd, term_type=None, encoding=None)
    await _wait_for_remote_socket(ssh_server, socket_path, server_proc)
    wrapper_pipe = None
    try:
        try:
            pipe = await ssh_server.open_unix_connection(socket_path)
            if hasattr(pipe, '_setup_connection'):
                await pipe._setup_connection()
        except asyncssh.misc.ChannelOpenError:
            if server_proc is not None:
                server_proc.terminate()
                try:
                    await asyncio.wait_for(server_proc.wait_closed(), timeout=5)
                except:
                    pass
                server_proc = None
            wrapper_pipe = await ssh_server.run_command(
                f"python3 /app/unix_socket_rpc_wrapper.py --socket {socket_path} --command python3 /app/rpc_server.py"
            )
            pipe = wrapper_pipe
        transformer = CborStreamTransformer().apply_transformer(pipe)
        yield transformer
    finally:
        try:
            if wrapper_pipe is not None:
                await wrapper_pipe.terminate()
            if server_proc is not None:
                server_proc.terminate()
                await asyncio.wait_for(server_proc.wait_closed(), timeout=5)
        except:
            pass

@pytest.mark.asyncio
async def test_rpc_all_methods_local_unix(unix_socket_pipe):
    rpc_client = RpcV1.read_only_client(unix_socket_pipe)
    await run_rpc_all_methods(rpc_client)

@pytest.mark.asyncio
async def test_rpc_all_methods_ssh_unix(ssh_unix_socket_pipe):
    rpc_client = RpcV1.read_only_client(ssh_unix_socket_pipe)
    await run_rpc_all_methods(rpc_client)

