import asyncio
import sys
from typing import Any, Dict, Literal, Optional, TypeVar

from cbor_rpc.pipe.aio_pipe import AioPipe

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
        process_stderr: Optional[asyncio.StreamReader] = None,
    ):
        super().__init__(reader, writer)
        self._process = process
        self._process_stderr = process_stderr
        self._stderr_task: Optional[asyncio.Task] = None

    async def _setup(self):
        await self._setup_connection()
        if self._process_stderr:
            self._stderr_task = asyncio.create_task(self._stderr_loop())

    async def _stderr_loop(self) -> None:
        if not self._process_stderr:
            return
        try:
            while self._connected and not self._closed:
                data = await self._process_stderr.read(self._chunk_size)
                if not data:
                    break
                await self._notify("stderr", data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._emit("error", e)

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
    async def start_process(
        cls,
        *args: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        stderr_mode: Literal["inherit", "pipe"] = "inherit",
    ) -> "StdioPipe":
        """
        Starts a process and returns a StdioPipe for it.

        Args:
            *args: Process argv.
            cwd: Optional working directory for subprocess.
            env: Optional environment variables for subprocess.
            stderr_mode:
                - "inherit": subprocess stderr writes to current process stderr.
                - "pipe": stderr is captured and emitted as "stderr" events.
        """
        stderr_target: Any = sys.stderr if stderr_mode == "inherit" else asyncio.subprocess.PIPE
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=stderr_target,
            cwd=cwd,
            env=env,
        )
        pipe = cls(process.stdout, process.stdin, process, process.stderr if stderr_mode == "pipe" else None)
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
        if self._stderr_task and not self._stderr_task.done():
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
            self._stderr_task = None
        if self._process and self._process.returncode is None:
            self._process.terminate()
        await super().terminate(*args)
