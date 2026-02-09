import asyncio
from typing import Any, List

import pytest

from cbor_rpc import EventPipe, RpcV1
from cbor_rpc.rpc.context import RpcCallContext

LOG_LEVELS = [
    ("crit", 1),
    ("warn", 2),
    ("info", 3),
    ("verbose", 4),
    ("debug", 5),
]


class LoggingServerRpc(RpcV1):
    def __init__(self, pipe: EventPipe[Any, Any], id_: str):
        super().__init__(pipe)
        self._id = id_

    def get_id(self) -> str:
        return self._id

    def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
        if method == "log_levels":
            for level_name, _level_value in LOG_LEVELS:
                self._log_for_level(context, f"method:{level_name}", level_name)
            return "ok"
        raise Exception(f"Unknown method: {method}")

    async def on_event(self, context: RpcCallContext, topic: str, message: Any) -> None:
        if message == "levels":
            for level_name, _level_value in LOG_LEVELS:
                self._log_for_level(context, f"event:{topic}:{level_name}", level_name)
            return
        context.logger.warn(f"event:{topic}:{message}")

    def _log_for_level(self, context: RpcCallContext, content: str, level_name: str) -> None:
        if level_name == "crit":
            context.logger.crit(content)
        elif level_name == "warn":
            context.logger.warn(content)
        elif level_name == "info":
            context.logger.log(content)
        elif level_name == "verbose":
            context.logger.verbose(content)
        elif level_name == "debug":
            context.logger.debug(content)
        else:
            raise Exception(f"Unknown log level: {level_name}")


class LoggingClientRpc(RpcV1):
    def __init__(self, pipe: EventPipe[Any, Any], id_: str):
        super().__init__(pipe)
        self._id = id_

    def get_id(self) -> str:
        return self._id

    def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
        raise Exception("Client Only Implementation")

    async def on_event(self, context: RpcCallContext, topic: str, message: Any) -> None:
        return None


async def _collect_logs(queue: asyncio.Queue, expected_count: int) -> List[List[Any]]:
    logs: List[List[Any]] = []
    for _ in range(expected_count):
        log = await asyncio.wait_for(queue.get(), timeout=1)
        logs.append(log)
    return logs


def _attach_log_listener(pipe: EventPipe[Any, Any], queue: asyncio.Queue) -> None:
    async def handler(chunk: Any) -> None:
        if not isinstance(chunk, list) or len(chunk) < 2:
            return
        if chunk[0] != 2 or chunk[1] == 0:
            return
        await queue.put(chunk)

    pipe.pipeline("data", handler)


@pytest.mark.asyncio
async def test_rpc_logging_all_levels_method_and_event():
    pipe_a, pipe_b = EventPipe.create_inmemory_pair()
    server = LoggingServerRpc(pipe_a, "server")
    client = LoggingClientRpc(pipe_b, "client")

    log_queue: asyncio.Queue = asyncio.Queue()
    _attach_log_listener(pipe_b, log_queue)

    await client.set_log_level(5)
    await asyncio.sleep(0.01)

    result = await client.call_method("log_levels")
    assert result == "ok"
    await client.emit("topic1", "levels")

    logs = await _collect_logs(log_queue, expected_count=10)

    expected = set()
    for level_name, level_value in LOG_LEVELS:
        expected.add((level_value, 1, 0, f"method:{level_name}"))
        expected.add((level_value, 3, "topic1", f"event:topic1:{level_name}"))

    actual = set((log[1], log[2], log[3], log[4]) for log in logs)
    assert actual == expected

    await pipe_a.terminate()
    await pipe_b.terminate()


@pytest.mark.asyncio
async def test_rpc_logging_respects_remote_level_setting():
    pipe_a, pipe_b = EventPipe.create_inmemory_pair()
    server = LoggingServerRpc(pipe_a, "server")
    client = LoggingClientRpc(pipe_b, "client")

    log_queue: asyncio.Queue = asyncio.Queue()
    _attach_log_listener(pipe_b, log_queue)

    await client.set_log_level(2)
    await asyncio.sleep(0.01)

    result = await client.call_method("log_levels")
    assert result == "ok"
    await client.emit("topic2", "levels")

    logs = await _collect_logs(log_queue, expected_count=4)
    actual_levels = {log[1] for log in logs}
    assert actual_levels == {1, 2}

    actual = set((log[1], log[2], log[3], log[4]) for log in logs)
    expected = {
        (1, 1, 0, "method:crit"),
        (2, 1, 0, "method:warn"),
        (1, 3, "topic2", "event:topic2:crit"),
        (2, 3, "topic2", "event:topic2:warn"),
    }
    assert actual == expected

    await pipe_a.terminate()
    await pipe_b.terminate()
