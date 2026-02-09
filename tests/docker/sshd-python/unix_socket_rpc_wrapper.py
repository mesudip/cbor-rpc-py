import argparse
import asyncio
import os
import sys
from typing import AsyncIterator, Optional

import cbor2

from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException


async def _iter_cbor(reader: asyncio.StreamReader) -> AsyncIterator[object]:
    transformer = CborStreamTransformer()
    while True:
        data = await reader.read(8192)
        if not data:
            break

        try:
            obj = await transformer.decode(data)
            yield obj
        except NeedsMoreDataException:
            pass

        while True:
            try:
                obj = await transformer.decode(None)
                yield obj
            except NeedsMoreDataException:
                break


async def _write_cbor(writer: asyncio.StreamWriter, obj: object, lock: asyncio.Lock) -> None:
    payload = cbor2.dumps(obj)
    async with lock:
        writer.write(payload)
        await writer.drain()


async def _forward_cbor_stream(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    lock: asyncio.Lock,
) -> None:
    async for obj in _iter_cbor(reader):
        await _write_cbor(writer, obj, lock)


async def _forward_process_stream(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    lock: asyncio.Lock,
    level: int,
    stream_name: str,
) -> None:
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            break
        text = chunk.decode("utf-8", errors="replace")
        log_message = [2, level, 1, 0, {"stream": stream_name, "text": text}]
        await _write_cbor(writer, log_message, lock)


async def _open_stdio() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    transport, proto = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
    writer = asyncio.StreamWriter(transport, proto, reader, loop)
    return reader, writer


async def _wait_for_socket(socket_path: str, timeout: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if os.path.exists(socket_path):
            return
        await asyncio.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for socket: {socket_path}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="CBOR RPC unix socket stdio wrapper")
    parser.add_argument("--socket", required=True, help="Unix socket path to connect")
    parser.add_argument("--command", nargs="+", help="Command to spawn the server")
    args = parser.parse_args()

    proc: Optional[asyncio.subprocess.Process] = None
    if args.command:
        proc = await asyncio.create_subprocess_exec(
            *args.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await _wait_for_socket(args.socket)

    client_reader, client_writer = await _open_stdio()
    sock_reader, sock_writer = await asyncio.open_unix_connection(args.socket)

    write_lock = asyncio.Lock()

    tasks = [
        asyncio.create_task(_forward_cbor_stream(client_reader, sock_writer, write_lock)),
        asyncio.create_task(_forward_cbor_stream(sock_reader, client_writer, write_lock)),
    ]

    if proc is not None and proc.stdout and proc.stderr:
        tasks.append(asyncio.create_task(_forward_process_stream(proc.stdout, client_writer, write_lock, 3, "stdout")))
        tasks.append(asyncio.create_task(_forward_process_stream(proc.stderr, client_writer, write_lock, 2, "stderr")))

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        sock_writer.close()
        await sock_writer.wait_closed()
        if proc is not None and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
