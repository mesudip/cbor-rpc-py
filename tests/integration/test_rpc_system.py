import asyncio
import os
import tempfile
import sys
import pytest
from contextlib import asynccontextmanager
from cbor_rpc import RpcV1
from cbor_rpc.pipe.aio_pipe import AioPipe
from cbor_rpc.stdio.stdio_pipe import StdioPipe
from cbor_rpc.ssh.ssh_pipe import SshServer
from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer


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


@asynccontextmanager
async def _stdio_pipe_ctx(server_script_path):
    # Start the server process via StdioPipe
    pipe = await StdioPipe.start_process(sys.executable, str(server_script_path))
    try:
        # Wrap the pipe with CBOR transformer
        yield CborStreamTransformer().apply_transformer(pipe)
    finally:
        # Ensure cleanup
        await pipe.terminate()
        try:
            await pipe.wait_for_process_termination()
        except Exception:
            pass


@asynccontextmanager
async def _unix_local_pipe_ctx():
    import uuid
    from pathlib import Path

    socket_path = Path(tempfile.gettempdir()) / f"cbor_rpc_test_{uuid.uuid4().hex[:8]}.sock"
    if socket_path.exists():
        socket_path.unlink()
    # Path to the actual server script
    server_script = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "docker", "sshd-python", "unix_socket_rpc_server.py")
    )
    # Start the server process
    proc = await asyncio.create_subprocess_exec(
        sys.executable, server_script, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
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


@asynccontextmanager
async def _unix_ssh_pipe_ctx(ssh_server):
    # Start the server process inside the container
    # the script is located at tests/docker/sshd-python
    server_cmd = "python3 /app/unix_socket_rpc_server.py"
    socket_path = "/tmp/cbor-rpc.sock"
    server_proc = await ssh_server._connection.create_process(server_cmd, term_type=None, encoding=None)
    await _wait_for_remote_socket(ssh_server, socket_path, server_proc)
    wrapper_pipe = None
    try:
        wrapper_pipe = await ssh_server.run_command(f"python3 /app/unix_socket_rpc_wrapper.py --socket {socket_path}")
        pipe = wrapper_pipe
        transformer = CborStreamTransformer().apply_transformer(pipe)
        yield transformer
    finally:
        try:
            await wrapper_pipe.terminate()
            if server_proc is not None:
                server_proc.terminate()
                await asyncio.wait_for(server_proc.wait_closed(), timeout=5)
        except:
            pass


@pytest.fixture(params=["stdio", "unix_local", "unix_ssh"])
async def rpc_client(request, get_stdio_server_script_path, ssh_server):
    if request.param == "stdio":
        async with _stdio_pipe_ctx(get_stdio_server_script_path) as pipe:
            yield RpcV1.read_only_client(pipe)
    elif request.param == "unix_local":
        async with _unix_local_pipe_ctx() as pipe:
            yield RpcV1.read_only_client(pipe)
    elif request.param == "unix_ssh":
        async with _unix_ssh_pipe_ctx(ssh_server) as pipe:
            yield RpcV1.read_only_client(pipe)
    else:
        raise ValueError(f"Unknown param: {request.param}")


@pytest.mark.asyncio
async def simple_rpc_calls(rpc_client):
    msg = "Hello World"
    assert await rpc_client.call_method("echo", msg) == msg

    assert await rpc_client.call_method("add", 1, 2) == 3
    assert await rpc_client.call_method("add", 10, 20, 30) == 60

    assert await rpc_client.call_method("multiply", 2, 3) == 6
    assert await rpc_client.call_method("multiply", 2, 3, 4) == 24

    rpc_client.set_timeout(30000)
    size = 1024 * 1024 * 4
    data = await rpc_client.call_method("download_random", size)
    assert isinstance(data, bytes)
    assert len(data) == size

    test_bytes = b"h" * size
    length = await rpc_client.call_method("upload_random", test_bytes)
    assert length == len(test_bytes)

    start = asyncio.get_running_loop().time()
    res = await rpc_client.call_method("sleep", 0.1)
    end = asyncio.get_running_loop().time()
    assert "Slept for 0.1 seconds" in res
    assert (end - start) >= 0.08

    res = await rpc_client.call_method("random_delay", 0.1)
    assert "Delayed for 0.1 seconds" in res
