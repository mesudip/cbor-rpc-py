import asyncio
import os
import sys
import logging

# Ensure cbor_rpc is in path
# Try to find where cbor_rpc is.
current_dir = os.path.dirname(os.path.abspath(__file__))
# If running from /usr/local/bin, we might need to look elsewhere.
# But Dockerfile copied cbor_rpc to ./cbor_rpc relative to WORKDIR.
# Let's blindly add possible locations
sys.path.append("/app")
sys.path.append("/")
sys.path.append(os.getcwd())
sys.path.insert(0, os.path.dirname(__file__))

from cbor_rpc.pipe.aio_pipe import AioPipe
from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer
from performance_server import PerformanceServer

logger = logging.getLogger(__name__)

async def handle_client(reader, writer):
    try:
        pipe = AioPipe(reader, writer)
        # Assuming we use CBOR transformer for binary efficiency
        rpc_pipe = CborStreamTransformer().apply_transformer(pipe)
        
        server = PerformanceServer()
        conn_id = str(id(writer))
        
        await server.add_connection(conn_id, rpc_pipe)
        await pipe._setup_connection()
        
        # Wait until pipe closes
        closed_event = asyncio.Event()
        rpc_pipe.on("close", lambda *_: closed_event.set())
        await closed_event.wait()
        
    except Exception as e:
        logger.error(f"Error handling client: {e}")
    finally:
        writer.close()

async def main():
    socket_path = "/tmp/cbor-rpc.sock"
    if os.path.exists(socket_path):
        os.remove(socket_path)
        
    server = await asyncio.start_unix_server(handle_client, path=socket_path)
    os.chmod(socket_path, 0o777) # Make sure everyone can access
    print(f"Server listening on {socket_path}")
    
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
