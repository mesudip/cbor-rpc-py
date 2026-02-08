import asyncio
import logging
import os
import sys
import subprocess
from typing import List, Any
from cbor_rpc import RpcV1Server
from cbor_rpc.rpc.context import RpcCallContext
from cbor_rpc.stdio.stdio_pipe import StdioPipe
from cbor_rpc.transformer.json_transformer import JsonStreamTransformer
from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer

# Configure logging to write to stderr so it doesn't interfere with stdout (which is used for RPC)
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

class RemoteServer(RpcV1Server):
    """
    A simple RPC server that exposes some methods.
    """
    async def validate_event_broadcast(self, connection_id, topic, message):
        return False

    async def handle_method_call(
        self,
        connection_id: str,
        context: RpcCallContext,
        method: str,
        args: List[Any],
    ) -> Any:
        context.logger.info(f"Received call: {method} {args}")
        if method == "echo":
            return args[0] if args else None
        elif method == "ls":
            path = args[0] if args else "."
            return os.listdir(path)
        elif method == "exec":
            cmd = args[0] if args else None
            if not cmd:
                 raise ValueError("exec requires a command argument")
            
            # Using context.logger to stream logs back to client.
            # We will run the process and capture stdout/stderr line by line.
            
            context.logger.info(f"Executing: {cmd}")
            
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            async  def read_stream(stream, log_func):
                 while True:
                    line = await stream.readline()
                    if not line:
                        break
                    decoded = line.decode().rstrip()
                    # Log each line via the RPC logger.
                    # This sends a "log" notification to the client.
                    log_func(decoded)
            
            # Collect streams concurrently
            await asyncio.gather(
                read_stream(process.stdout, context.logger.info),
                read_stream(process.stderr, context.logger.warn)
            )
            
            return await process.wait()

        elif method == "whoami":
            import getpass
            return getpass.getuser()
        else:
            raise ValueError(f"Unknown method: {method}")

async def main():
    # 1. Open Stdio Pipe (connects to stdin/stdout)
    try:
        pipe = await StdioPipe.open()
    except Exception as e:
        logging.error(f"Failed to open stdio pipe: {e}")
        return

    # 2. Add Transformer (JSON or CBOR). 
    # Use JSON for readability in debug, or CBOR for efficiency.
    # The client must match this. 
    # Let's support an argument to choose, defaulting to JSON for this example.
    
    # We will use JSON for this example to make it easier to debug via raw ssh if needed.
    # rpc_pipe = CborStreamTransformer().apply_transformer(pipe)
    rpc_pipe = JsonStreamTransformer().apply_transformer(pipe)

    # 3. Create Server
    server = RemoteServer()
    
    # 4. Attach pipe to server
    connection_id = "stdio-client"
    await server.add_connection(connection_id, rpc_pipe)
    
    logging.info("Stdio RPC Server running...")
    
    # Keep running until the pipe closes
    # When the ssh connection closes, stdin/stdout close, pipe emits 'close'.
    
    close_event = asyncio.Event()
    
    def on_close():
        logging.info("Pipe closed. Shutting down.")
        close_event.set()
        
    rpc_pipe.on("close", lambda *_: on_close())
    
    await close_event.wait()

if __name__ == "__main__":
    asyncio.run(main())
