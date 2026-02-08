import asyncio
import os
import sys
from pathlib import Path

# Ensure cbor_rpc is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append("/app")
sys.path.append("/")
sys.path.append(os.getcwd())

from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer
from cbor_rpc.stdio.stdio_pipe import StdioPipe

performance_dir = Path(__file__).resolve().parents[1] / "docker" / "sshd-python"
sys.path.append(str(performance_dir))
from performance_server import PerformanceServer


async def main_stdio():
    try:
        pipe = await StdioPipe.open()
        rpc_pipe = CborStreamTransformer().apply_transformer(pipe)
        server = PerformanceServer()
        await server.add_connection("stdio-client", rpc_pipe)
        await asyncio.Event().wait()
    except Exception as e:
        sys.stderr.write(f"Server error: {e}\n")


if __name__ == "__main__":
    asyncio.run(main_stdio())
