import asyncio
import sys
from typing import Any, List, Optional, Tuple, TypeVar, Generic

from cbor_rpc.pipe.aio_pipe import AioPipe
from cbor_rpc.pipe.pipe import Pipe
from cbor_rpc.event.emitter import AbstractEmitter

T1 = TypeVar("T1")
T2 = TypeVar("T2")


class StdioPipe(AioPipe[bytes, bytes]):
    """
    A Pipe implementation that works with asyncio.StreamReader and asyncio.StreamWriter
    typically obtained from a subprocess's stdin/stdout.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        process: Optional[asyncio.subprocess.Process] = None,
    ):
        super().__init__(reader, writer)
        self._process = process

    async def _setup(self):
        await self._setup_connection()

    @classmethod
    async def open(cls) -> "StdioPipe":
        """
        Creates a StdioPipe from the process's stdin and stdout.
        """
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        writer_transport, writer_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
        writer = asyncio.StreamWriter(writer_transport, writer_protocol, reader, loop)
        pipe = cls(reader, writer)
        await pipe._setup()
        return pipe

    @classmethod
    async def start_process(cls, *args: str) -> "StdioPipe":
        """
        Starts a process and returns a StdioPipe for it.
        """
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=sys.stderr,
        )
        pipe = cls(process.stdout, process.stdin, process)
        await pipe._setup()
        return pipe

    async def wait_for_process_termination(self) -> int:
        """
        Waits for the started subprocess to terminate and returns its exit code.
        Raises RuntimeError if no process was started by this pipe.
        """
        if not self._process:
            raise RuntimeError("No subprocess associated with this StdioPipe instance.")
        return await self._process.wait()

    async def terminate(self, *args: Any):
        """
        Terminates the started subprocess if one exists.
        """
        if self._process and self._process.returncode is None:
            self._process.terminate()
        await super().terminate(*args)
