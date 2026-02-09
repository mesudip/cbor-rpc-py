import asyncio
from pathlib import Path


def get_stdio_server_script_path() -> Path:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "stdio_rpc_server.py"
    if not script_path.exists():
        raise FileNotFoundError("Missing tests/scripts/stdio_rpc_server.py")
    return script_path


async def run_rpc_all_methods(client):
    msg = "Hello World"
    assert await client.call_method("echo", msg) == msg

    assert await client.call_method("add", 1, 2) == 3
    assert await client.call_method("add", 10, 20, 30) == 60

    assert await client.call_method("multiply", 2, 3) == 6
    assert await client.call_method("multiply", 2, 3, 4) == 24

    client.set_timeout(30000)
    size = 1024 * 1024 * 4
    data = await client.call_method("download_random", size)
    assert isinstance(data, bytes)
    assert len(data) == size

    test_bytes = b"h" * size
    length = await client.call_method("upload_random", test_bytes)
    assert length == len(test_bytes)

    start = asyncio.get_running_loop().time()
    res = await client.call_method("sleep", 0.1)
    end = asyncio.get_running_loop().time()
    assert "Slept for 0.1 seconds" in res
    assert (end - start) >= 0.08

    res = await client.call_method("random_delay", 0.1)
    assert "Delayed for 0.1 seconds" in res
