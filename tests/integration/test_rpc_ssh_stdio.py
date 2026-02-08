import pytest
from cbor_rpc import RpcV1
from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer
from tests.integration.rpc_test_helpers import get_stdio_server_script_path, run_rpc_all_methods


@pytest.mark.asyncio
async def test_rpc_ssh_stdio_all_methods(ssh_server):
    """
    Tests RPC interactions over an SSH Stdio tunnel.
    Steps:
    1. Upload the custom Stdio-enabled server script to the container.
    2. Run the script via SSH exec.
    3. Treat the SSH channel as a pipe (stdin/stdout) for RPC.
    """
    remote_script_path = "/tmp/stdio_rpc_server_ssh.py"

    # 1. Upload Script
    conn = ssh_server._connection
    server_code = get_stdio_server_script_path().read_text(encoding="utf-8")
    async with conn.start_sftp_client() as sftp:
        async with sftp.open(remote_script_path, "w") as f:
            await f.write(server_code)

    # 2. Run Command
    pipe = await ssh_server.run_command(f"python3 {remote_script_path}")

    try:
        # 3. Apply Transformer
        rpc_pipe = CborStreamTransformer().apply_transformer(pipe)
        client = RpcV1.read_only_client(rpc_pipe)
        await run_rpc_all_methods(client)

    finally:
        await pipe.terminate()
