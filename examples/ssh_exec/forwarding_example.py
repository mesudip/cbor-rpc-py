import asyncio
import asyncssh
import os

async def run_forwarding_example(host, username):
    # 1. Start a dummy "server" on the remote machine (simulation)
    # We will simulate a service listening on a Unix Socket on the remote machine
    # In reality, this would be your actual application server.
    
    # Connect
    conn = await asyncssh.connect(host, username=username)
    
    # We'll run a command that creates a listening unix socket using python on remote
    remote_socket_path = "/tmp/cbor_rpc_remote.sock"
    
    # Simple python script to listen on unix socket and echo back
    remote_server_script = f"""
import asyncio, os
from pathlib import Path

async def handle(reader, writer):
    data = await reader.read(100)
    writer.write(b"RemoteSocket received: " + data)
    await writer.drain()
    writer.close()

async def main():
    if os.path.exists('{remote_socket_path}'):
        os.remove('{remote_socket_path}')
        
    server = await asyncio.start_unix_server(handle, path='{remote_socket_path}')
    Path('{remote_socket_path}').chmod(0o777)
    print("Listening on {remote_socket_path}")
    await server.serve_forever()

asyncio.run(main())
"""
    # Start remote server in background
    print(f"Starting remote server on {remote_socket_path}...")
    remote_proc = await conn.create_process(f"python3 -c \"{remote_server_script}\"")
    
    # Give it a moment to start
    await asyncio.sleep(2)
    
    # 2. Direct Connection to Remote Unix Socket
    # We don't need to create a local socket file or use forward_local_path.
    # We can ask SSH to open a direct channel to the remote socket.
    
    print(f"Connecting directly to remote socket: {remote_socket_path}")
    
    try:
        # conn.create_unix_connection returns (reader, writer) just like asyncio.open_unix_connection
        # but the connection happens on the remote machine.
        reader, writer = await conn.create_unix_connection(remote_socket_path)
        
        msg = b"Hello via Direct SSH Stream!"
        print(f"Sending: {msg}")
        writer.write(msg)
        await writer.drain()
        
        response = await reader.read(1000)
        print(f"Response: {response.decode()}")
        
        writer.close()
        await writer.wait_closed()
        
    except Exception as e:
        print(f"Connection failed: {e}")
        
    finally:
        # Cleanup
        await remote_proc.terminate()
        conn.close()

if __name__ == "__main__":
    # Adjust host/user as needed for testing
    # asyncio.run(run_forwarding_example("localhost", "user"))
    print("This script demonstrates concept code for SSH Unix Socket Forwarding.")
