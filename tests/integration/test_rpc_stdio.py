import sys
import pytest
from cbor_rpc import RpcV1
from cbor_rpc.stdio.stdio_pipe import StdioPipe
from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer
from tests.integration.rpc_test_helpers import  run_rpc_all_methods



@pytest.mark.asyncio
async def test_rpc_stdio_all_methods(get_stdio_server_script_path):
    # Start the server process via StdioPipe
    pipe = await StdioPipe.start_process(sys.executable, str(get_stdio_server_script_path))
    
    try:
        # Wrap the pipe with CBOR transformer
        rpc_pipe = CborStreamTransformer().apply_transformer(pipe)
        client = RpcV1.read_only_client(rpc_pipe)
        await run_rpc_all_methods(client)

    finally:
        # Ensure cleanup
        await pipe.terminate()
        try:
            await pipe.wait_for_process_termination()
        except Exception:
            pass
