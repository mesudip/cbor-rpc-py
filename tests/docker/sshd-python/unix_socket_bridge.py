import asyncio
import sys


async def _pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        data = await reader.read(8192)
        if not data:
            break
        writer.write(data)
        await writer.drain()


async def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: unix_socket_bridge.py /path/to/socket")

    socket_path = sys.argv[1]

    loop = asyncio.get_running_loop()
    stdin_reader = asyncio.StreamReader()
    stdin_protocol = asyncio.StreamReaderProtocol(stdin_reader)
    await loop.connect_read_pipe(lambda: stdin_protocol, sys.stdin)

    stdout_transport, stdout_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
    stdout_writer = asyncio.StreamWriter(stdout_transport, stdout_protocol, stdin_reader, loop)

    sock_reader, sock_writer = await asyncio.open_unix_connection(socket_path)

    await asyncio.gather(
        _pump(stdin_reader, sock_writer),
        _pump(sock_reader, stdout_writer),
    )


if __name__ == "__main__":
    asyncio.run(main())
