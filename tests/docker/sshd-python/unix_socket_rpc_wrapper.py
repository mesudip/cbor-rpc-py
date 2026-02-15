import argparse
import asyncio
import os
import sys
import traceback
from typing import AsyncIterator, Optional

# Ensure cbor_rpc is in path
sys.path.append("/app")

import cbor2

from cbor_rpc.transformer.cbor_transformer import CborStreamTransformer
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException

# Queue for Redirected logs
log_queue: asyncio.Queue = asyncio.Queue()


class LogRedirector:
    def __init__(self, level: int):
        self.level = level

    def write(self, message: str) -> None:
        if not message:
            return
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.call_soon(log_queue.put_nowait, (self.level, message))
        except RuntimeError:
            pass

    def flush(self) -> None:
        pass


async def _log_worker(writer: asyncio.StreamWriter, lock: asyncio.Lock) -> None:
    while True:
        level, message = await log_queue.get()
        # [2, 0, ID=None, Level, Content]
        # We send it as a system log (ID=0)
        obj = [2, 0, None, level, message]
        try:
            await _write_cbor(writer, obj, lock)
        except Exception:
            break


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
        # Standard RPC Log: [2, 0, 0, level, content]
        log_message = [2, 0, 0, level, {"stream": stream_name, "text": text}]
        await _write_cbor(writer, log_message, lock)


async def _open_stdio() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    # Use sys.__stdin__.buffer for binary read
    if sys.__stdin__ is None:
        raise RuntimeError("sys.__stdin__ is None")
    await loop.connect_read_pipe(lambda: protocol, sys.__stdin__.buffer)

    # Use sys.__stdout__.buffer for binary write
    if sys.__stdout__ is None:
        raise RuntimeError("sys.__stdout__ is None")
    transport, proto = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.__stdout__.buffer)
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
    # Redirect stdout/stderr to log queue
    sys.stdout = LogRedirector(3)  # Info
    sys.stderr = LogRedirector(2)  # Warning

    try:
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

        sock_reader, sock_writer = None, None
        for i in range(10):
            try:
                sock_reader, sock_writer = await asyncio.open_unix_connection(args.socket)
                break
            except (ConnectionRefusedError, FileNotFoundError):
                if i == 9:
                    raise
                await asyncio.sleep(0.5)

        client_write_lock = asyncio.Lock()
        sock_write_lock = asyncio.Lock()

        tasks = [
            asyncio.create_task(_forward_cbor_stream(client_reader, sock_writer, sock_write_lock)),
            asyncio.create_task(_forward_cbor_stream(sock_reader, client_writer, client_write_lock)),
            asyncio.create_task(_log_worker(client_writer, client_write_lock)),
        ]

        if proc is not None and proc.stdout and proc.stderr:
            tasks.append(
                asyncio.create_task(_forward_process_stream(proc.stdout, client_writer, client_write_lock, 3, "stdout"))
            )
            tasks.append(
                asyncio.create_task(_forward_process_stream(proc.stderr, client_writer, client_write_lock, 2, "stderr"))
            )

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
    except Exception:
        # Capture unhandled exceptions and send as log
        e_type, e_value, e_tb = sys.exc_info()
        lines = traceback.format_exception(e_type, e_value, e_tb)
        text = "".join(lines)
        # Write to original stderr to ensure it escapes the container/process even if loop dies
        if sys.__stderr__:
            sys.__stderr__.write(f"Wrapper Crash: {text}\n")
            sys.__stderr__.flush()
        # Also try to log if possible (best effort)
        try:
            if sys.stderr and hasattr(sys.stderr, "write"):
                sys.stderr.write(f"Wrapper Crash: {text}")
        except:
            pass
        # Wait a bit
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
