import asyncio
import argparse
import sys
from cbor_rpc.ssh.ssh_pipe import SshServer

async def main():
    parser = argparse.ArgumentParser(description="Execute a remote command using SshPipe.")
    parser.add_argument("--host", required=True, help="SSH Host")
    parser.add_argument("--port", type=int, default=22, help="SSH Port")
    parser.add_argument("--user", required=True, help="SSH Username")
    parser.add_argument("--password", help="SSH Password")
    parser.add_argument("command", help="Command to execute")
    
    args = parser.parse_args()
    
    print(f"Connecting to {args.user}@{args.host}:{args.port}...")
    
    server = SshServer(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
    )

    try:
        await server.connect()
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    print(f"Connected. Executing: {args.command}")
    
    try:
        pipe = await server.run_command(args.command)
    except Exception as e:
        print(f"Failed to run command: {e}")
        await server.close()
        return

    # Setup callback to print data
    loop = asyncio.get_running_loop()
    done_event = asyncio.Event()

    def on_data(data: bytes):
        # Decode and print to stdout
        try:
            print(data.decode(), end="")
        except UnicodeDecodeError:
            print(data)

    def on_close(*args):
        done_event.set()

    def on_error(err):
        print(f"Error: {err}", file=sys.stderr)
        done_event.set()

    pipe.pipeline("data", on_data)
    pipe.on("close", on_close)
    pipe.on("error", on_error)

    # Wait for the command to finish (remote closes the channel)
    await done_event.wait()
    await pipe.terminate()
    await server.close()

if __name__ == "__main__":
    asyncio.run(main())
