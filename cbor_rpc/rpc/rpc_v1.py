import sys
import logging
from typing import Any, Dict, List, Optional, Callable
from abc import abstractmethod
import asyncio

from .rpc_base import RpcClient
from .context import RpcCallContext
from .logging import RpcLogger
from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.timed_promise import TimedPromise

logger = logging.getLogger(__name__)


class RpcCore(RpcClient):
    protocol_id = 1
    def __init__(self, pipe: EventPipe[Any, Any]):
        self.pipe = pipe
        self._counter = 0
        self._promises: Dict[int, TimedPromise] = {}
        self._timeout = 30000
        self._peer_log_level = 0
        self._desired_log_level = 0
        self.logger = RpcLogger(self._send_log, 1, self._get_default_ref_id)

        async def resolve_result(result: Any) -> Any:
            """Recursively resolve coroutines or nested coroutines."""
            while asyncio.iscoroutine(result):
                result = await result
            return result

        self._resolve_result = resolve_result

        async def on_data(data: List[Any]) -> None:
            try:
                if not isinstance(data, list) or len(data) < 2:
                    logger.warning(f"RpcCore: Invalid message format: {data}")
                    return

                protocol_id = data[0]
                if protocol_id == 1:
                    await self.handle_proto_1(data)
                elif protocol_id == 2:
                    await self.handle_proto_2(data)
                elif protocol_id == 3:
                    await self.handle_proto_3(data)
                else:
                    logger.warning(f"RpcCore: Unsupported protocol: {data}")

            except Exception as e:
                logger.error(f"Error processing RPC message: {e}")

        self.pipe.on("data", on_data)

    async def handle_proto_1(self, data: List[Any]) -> None:
        """Handle Protocol 1 (RPC) messages."""
        if len(data) < 3:
            logger.warning(f"RpcCore [Proto 1]: Invalid format: {data}")
            return

        sub_proto_id = data[1]

        # Responses: [1, 0, id, result] (Success) or [1, 1, id, error] (Error)
        if sub_proto_id <2:
            if len(data) < 4:
                logger.warning(f"RpcCore [Proto 1]: Invalid response format: {data}")
                return

            id_ = data[2]
            payload = data[3]

            promise = self._promises.pop(id_, None)
            if promise:
                if sub_proto_id == 0:  # Success
                    await promise.resolve(payload)
                else:  # Error
                    await promise.reject(payload)
            else:
                logger.warning(f"Received rpc reply for expired request id: {id_}, success={sub_proto_id==0}, data={payload}")

        # Method Call (2) or Fire (3): [1, 2/3, id, method, params]
        elif sub_proto_id == 2 or sub_proto_id == 3:
            if len(data) < 5:
                logger.warning(f"RpcCore [Proto 1]: Invalid call format: {data}")
                return

            id_ = data[2]
            method = data[3]
            params = data[4]

            try:
                # Call the method
                context = RpcCallContext(self.logger)
                result = self.handle_method_call(context, method, params)

                if sub_proto_id == 2:  # Expect Response
                    async def handle_response() -> None:
                        try:
                            resolved_result = await self._resolve_result(result)
                            # Send Success: [1, 0, id, result]
                            await self.pipe.write([1, 0, id_, resolved_result])
                        except Exception as e:
                            # Send Error: [1, 1, id, error]
                            await self.pipe.write([1, 1, id_, str(e)])

                    asyncio.create_task(handle_response())
                else:
                    # Fire (3)
                    async def handle_fire() -> None:
                        try:
                            await self._resolve_result(result)
                        except Exception as e:
                            logger.error(f"Fired method error: {method}, params={params}, error={e}")
                    asyncio.create_task(handle_fire())

            except Exception as e:
                if sub_proto_id == 2:
                    asyncio.create_task(self.pipe.write([1, 1, id_, str(e)]))
                else:
                    logger.error(f"Fired method error: {method}, params={params}, error={e}")
        else:
            logger.warning(f"RpcCore [Proto 1]: Unknown sub-protocol: {sub_proto_id}")


    async def handle_proto_2(self, data: List[Any]) -> None:
        """Handle Protocol 2 (Logging) messages."""
        # Format: [2, log_level, ref_proto, ref_id, content]
        if len(data) >= 3 and data[1] == 0:
            self._peer_log_level = data[2]
            return

        if len(data) < 5:
            logger.warning(f"RpcCore [Proto 2]: Invalid format: {data}")
            return

        log_level = data[1]

        ref_proto = data[2]
        ref_id = data[3]
        content = data[4]

        level_map = {1: "CRITICAL", 2: "WARN", 3: "INFO", 4: "VERBOSE", 5: "DEBUG"}
        level_str = level_map.get(log_level, f"LEVEL-{log_level}")

        logger.info(f"[RemoteLog:{level_str}] p{ref_proto}:{ref_id} {content}")

    async def handle_proto_3(self, data: List[Any]) -> None:
        logger.warning(f"RpcCore [Proto 3]: Unsupported event message: {data}")


    async def call_method(self, method: str, *args: Any) -> Any:
        counter = self._counter
        self._counter += 1

        def timeout_callback():
            self._promises.pop(counter, None)

        promise = TimedPromise(self._timeout, timeout_callback)
        self._promises[counter] = promise
        # New: [1, 2, counter, method, args]
        await self.pipe.write([1, 2, counter, method, list(args)])
        return await promise.promise

    async def fire_method(self, method: str, *args: Any) -> None:
        counter = self._counter
        self._counter += 1
        # New: [1, 3, counter, method, args]
        await self.pipe.write([1, 3, counter, method, list(args)])

    def set_timeout(self, milliseconds: int) -> None:
        self._timeout = milliseconds

    async def set_log_level(self, level: int) -> None:
        """Send a request to set the remote log level."""
        # Protocol 2, Sub-protocol 0: [2, 0, level]
        self._desired_log_level = level
        await self.pipe.write([2, 0, level])

    def _get_default_ref_id(self) -> int:
        return 0

    def _send_log(self, level: int, ref_proto: int, ref_id: Any, content: Any) -> None:
        if self._peer_log_level <= 0:
            return
        if level > self._peer_log_level:
            return
        asyncio.create_task(self.pipe.write([2, level, ref_proto, ref_id, content]))

    @abstractmethod
    def get_id(self) -> str:
        pass

    @abstractmethod
    def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
        pass

    @classmethod
    def make_rpc_v1(
        cls,
        pipe: EventPipe[Any, Any],
        id_: str,
        method_handler: Callable,
    ) -> "RpcCore":
        class ConcreteRpcV1(cls):
            def get_id(self) -> str:
                return id_

            def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
                return method_handler(context, method, args)

            async def on_event(self, context: RpcCallContext, topic: str, message: Any) -> None:
                return None

        return ConcreteRpcV1(pipe)

    @classmethod
    def read_only_client(cls, pipe: EventPipe[Any, Any]) -> "RpcCore":
        def method_handler(context: RpcCallContext, method: str, args: List[Any]) -> Any:
            raise Exception("Client Only Implementation")

        return cls.make_rpc_v1(pipe, "", method_handler)


class RpcV1(RpcCore):
    def __init__(self, pipe: EventPipe[Any, Any]):
        super().__init__(pipe)
        self._waiters: Dict[str, TimedPromise] = {}
        self._last_event_topic: Optional[str] = None
        self.event_logger = RpcLogger(self._send_log, 3, self._get_last_event_topic)

    async def handle_proto_3(self, data: List[Any]) -> None:
        if len(data) < 4:
            logger.warning(f"RpcV1 [Proto 3]: Invalid event format: {data}")
            return
        sub_proto_id = data[1]
        if sub_proto_id != 0:
            logger.warning(f"RpcV1 [Proto 3]: Unknown sub-protocol: {sub_proto_id}")
            return
        await self._on_event(data[2], data[3])

    async def emit(self, topic: str, args: Any) -> None:
        await self.pipe.write([3, 0, topic, args])

    async def _on_event(self, method: str, message: Any) -> None:
        self._last_event_topic = method
        waiter = self._waiters.pop(method, None)
        if waiter:
            await waiter.resolve(message)
        else:
            context = RpcCallContext(self.event_logger)
            await self.on_event(context, method, message)

    async def wait_next_event(self, topic: str, timeout_ms: Optional[int] = None) -> Any:
        if topic in self._waiters:
            raise Exception("Already waiting for event")

        def timeout_callback():
            self._waiters.pop(topic, None)

        waiter = TimedPromise(
            timeout_ms or self._timeout,
            timeout_callback,
            f"Timeout Waiting for Event on: {topic}",
        )
        self._waiters[topic] = waiter
        return await waiter.promise

    @abstractmethod
    async def on_event(self, context: RpcCallContext, topic: str, message: Any) -> None:
        pass

    def _get_last_event_topic(self) -> Any:
        return self._last_event_topic
