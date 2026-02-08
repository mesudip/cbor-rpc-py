import asyncio
import argparse
import sys
from cbor_rpc.ssh.ssh_pipe import SshServer
from cbor_rpc.rpc import RpcClient
from cbor_rpc.transformer.json_transformer import JsonStreamTransformer


async def main():
    parser = argparse.ArgumentParser(description="Run RPC over SSH.")
    parser.add_argument("--host", required=True, help="SSH Host")
    parser.add_argument("--port", type=int, default=22, help="SSH Port")
    parser.add_argument("--user", required=True, help="SSH Username")
    parser.add_argument("--password", help="SSH Password")
    parser.add_argument(
        "--remote-cmd",
        default="python3 examples/ssh_exec/stdio_rpc_server.py",
        help="Command to run RPC server on remote machine. Ensure the file exists.",
    )

    args = parser.parse_args()

    print(f"Connecting to {args.user}@{args.host}:{args.port}...")

    try:
        # Establish the pipe
        server = SshServer(
            host=args.host,
            port=args.port,
            username=args.user,
            password=args.password,
        )
        await server.connect()
        pipe = await server.run_command(args.remote_cmd)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    print("SSH transport established. Initializing RPC...")

    # Wrap the pipe with a transformer (JSON)
    # The result is a pipe that inputs/outputs objects (lists/dicts)
    json_pipe = JsonStreamTransformer().apply_transformer(pipe)

    # Create RPC Client using the SSH pipe
    client = RpcClient(json_pipe)

    # -------------------------------------------------------------
    # Setup Remote Server Log Monitoring (Stderr)
    # The remote server script logs to stderr. We can read that via the SSH channel.
    # -------------------------------------------------------------
    async def monitor_remote_stderr():
        if not hasattr(pipe, "_ssh_channel") or not pipe._ssh_channel:
            return

        stderr_stream = pipe._ssh_channel.stderr
        if not stderr_stream:
            return

        try:
            # asyncssh streams can be iterated line by line
            async for line in stderr_stream:
                # We can strip the line or keep it as is. Logging usually adds newline.
                # Assuming bytes or str depending on encoding. connect() used encoding=None, so it's bytes.
                decoded = line.decode(errors="replace") if isinstance(line, bytes) else line
                print(f"[REMOTE SERVER LOG] {decoded.strip()}", file=sys.stderr)
        except Exception as e:
            print(f"Error reading remote stderr: {e}", file=sys.stderr)

    # Start the monitoring task
    stderr_task = asyncio.create_task(monitor_remote_stderr())

    # -------------------------------------------------------------
    # Setup Log Handling (RPC Protocol Logs)
    # The server uses context.logger.info/warn/etc to send logs.
    # We can subscribe to these log events on the RpcClient.
    # -------------------------------------------------------------
    def on_log(level, content):
        prefix = "[RPC LOG]"
        if level <= 2:  # warn/crit
            print(f"{prefix} ERROR: {content}", file=sys.stderr)
        else:
            print(f"{prefix} INFO: {content}")

    client.set_on_log(on_log)

    try:
        # Example RPC call.
        print(f"Calling 'exec' on remote: {args.remote_cmd}")

        # We will execute a simple echo command to demonstrate log streaming
        # Note: The remote-cmd argument above starts the SERVER.
        # Now we are calling the 'exec' method *on* that server.
        cmd_to_run = "echo 'Hello from inside subprocess'; sleep 1; echo 'Done sleeping'"

        print(f">>> Calling remote exec('{cmd_to_run}')")
        exit_code = await client.request("exec", [cmd_to_run])
        print(f"<<< Remote exec finished with exit code: {exit_code}")

        print("\nCalling 'ls' on remote...")
        files = await client.request("ls", ["."])
        print("Remote files:")
        for f in files:
            print(f" - {f}")

    except Exception as e:
        print(f"RPC Call failed: {e}")

    # Clean up
    await client.close()

    # Wait for stderr task to finish? It might block if server doesn't close stderr.
    # Usually closing client/pipe closes the session which closes stderr.
    stderr_task.cancel()
    try:
        await stderr_task
    except asyncio.CancelledError:
        pass

    await server.close()
    print("Closed.")


if __name__ == "__main__":
    asyncio.run(main())
