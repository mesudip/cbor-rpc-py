import asyncio
from typing import Tuple


async def create_stream_pair() -> Tuple[asyncio.AbstractServer, asyncio.StreamReader, asyncio.StreamWriter]:
    async def handler(_reader: asyncio.StreamReader, _writer: asyncio.StreamWriter) -> None:
        await asyncio.sleep(0.2)

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    reader, writer = await asyncio.open_connection(host, port)
    return server, reader, writer
