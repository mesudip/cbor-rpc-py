import sys
from typing import Any, Dict, List, Optional, Callable
from abc import ABC, abstractmethod
import asyncio
import inspect
import logging

from .rpc_base import RpcInitClient
from .rpc_call import RpcCallHandle
from .logging import RpcLogger
from .context import RpcCallContext, current_request_id
from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.timed_promise import TimedPromise

logger = logging.getLogger(__name__)


class RpcV1(RpcInitClient):
    protocol_id = 1

    def __init__(self, pipe: EventPipe[Any, Any]):
        self.pipe = pipe
        self._counter = 0
        self._active_calls: Dict[int, RpcCallHandle] = {}
        self._incoming_contexts: Dict[int, RpcCallContext] = {}
        self._timeout = 30000
        self._waiters: Dict[str, TimedPromise] = {}
        self._peer_log_level = 5  # Default to Debug for Godlike logging

        # Initialize Logger
        # For outgoing logs (Server -> Client), ID should reference the REQUEST ID if currently handling one?
        # Or just use 0 if global?
        # For now, simplistic logger attached to instance.
        self.logger = RpcLogger(self.pipe, lambda: 0, self._peer_log_level)
        # Lambda 0 is placeholder. Ideally context contextvars would be used to get current request ID.

        self.pipe.pipeline("data", self._on_data)

    async def _resolve_result(self, result: Any) -> Any:
        """Recursively resolve coroutines or nested coroutines."""
        while asyncio.iscoroutine(result):
            result = await result
        return result

    async def _handle_stream_message(self, data: List[Any]) -> None:
        sub_protocol = data[1]
        # Protocol 2: Streaming (Logs/Progress)
        if len(data) != 5:
            return
        id_, method, params = data[2], data[3], data[4]
        handle = self._active_calls.get(id_)
        if handle:
            if sub_protocol == 0:  # Log
                handle._emit_log(method, params)
            elif sub_protocol == 1:  # Progress
                handle._emit_progress(method, params)
        else:
            if sub_protocol == 0:
                # If we receive a log for unknown ID (or 0), we might want to log it to system
                logger.info(f"Received stream log for unknown ID {id_}: {params}")
                if id_ == 0:
                    print(f"[Remote System Log] {params}", file=sys.stderr)
            else:
                logger.warning(f"Received stream message for unknown/inactive ID {id_}: {data}")

    async def _handle_event_message(self, data: List[Any]) -> None:
        # Protocol 3: Events [3, 0, Topic, Message]
        if len(data) != 4:
            return
        topic, message = data[2], data[3]

        # Resolve waiters
        waiter = self._waiters.pop(topic, None)
        if waiter:
            await waiter.resolve(message)

        # Call handler
        asyncio.create_task(self.on_event(topic, message))

    async def _handle_rpc_message(self, data: List[Any]) -> None:
        sub_protocol = data[1]

        if sub_protocol == 3:  # Cancel
            if len(data) != 3:
                return
            id_ = data[2]
            ctx = self._incoming_contexts.get(id_)
            if ctx:
                ctx.cancelled = True
            return

        if len(data) != 5:
            logger.warning(f"Invalid message format for Proto 1: {data}")
            return
        id_, method, params = data[2], data[3], data[4]

        if sub_protocol < 2:  # Method call (0) or fire (1)
            token = current_request_id.set(id_)
            try:
                # Create context
                def emit_progress(value: Any, meta: Any = None):
                    # Progress Protocol: [2, 1, ID, Value, Metadata]
                    asyncio.create_task(self.pipe.write([2, 1, id_, value, meta]))

                request_logger = RpcLogger(
                    self.pipe,
                    lambda: id_,
                    self._peer_log_level,
                )

                context = RpcCallContext(id_, request_logger, emit_progress)
                self.register_incoming_context(id_, context)

                # Call the method and get the result
                result = self.handle_method_call(context, method, params)

                # Handle the response asynchronously
                async def handle_response():
                    try:
                        resolved_result = await self._resolve_result(result)
                        if sub_protocol == 0:  # Only respond to method calls, not fire calls
                            await self.pipe.write([1, 2, id_, True, resolved_result])
                    except Exception as e:
                        if sub_protocol == 0:
                            await self.pipe.write([1, 2, id_, False, str(e)])
                        else:
                            logger.error(f"Fired method error: {method}, params={params}, error={e}")
                    finally:
                        self._incoming_contexts.pop(id_, None)

                # Create task to handle response
                asyncio.create_task(handle_response())

            except Exception as e:
                self._incoming_contexts.pop(id_, None)
                if sub_protocol == 0:
                    asyncio.create_task(self.pipe.write([1, 2, id_, False, str(e)]))
                else:
                    logger.error(f"Fired method error: {method}, params={params}, error={e}")
            finally:
                current_request_id.reset(token)

        elif sub_protocol == 2:  # Response
            handle = self._active_calls.pop(id_, None)
            if handle:
                promise = handle._promise
                if method is True:  # Success
                    await promise.resolve(params)
                else:  # Error
                    await promise.reject(params)
            else:
                # Fallback for old promises if any? No.
                logger.warning(f"Received rpc reply for expired request id: {id_}, success={method}, data={params}")
        else:
            logger.warning(f"RpcV1: Invalid direction: {sub_protocol}")

    async def _on_data(self, data: List[Any]) -> None:
        try:
            if not isinstance(data, list) or len(data) < 3:
                logger.warning(f"Invalid message format: {data}")
                return

            protocol_id = data[0]

            if protocol_id == 2:
                await self._handle_stream_message(data)
                return

            if protocol_id == 3:
                await self._handle_event_message(data)
                return

            if protocol_id == 1:
                await self._handle_rpc_message(data)
                return

            logger.warning(f"Unsupported protocol: {data}")
        except Exception as e:
            logger.error(f"Error in on_data: {e}", exc_info=True)

    def create_call(self, method: str, *args: Any) -> RpcCallHandle:
        counter = self._counter
        self._counter += 1

        def start_callback(handle: RpcCallHandle) -> TimedPromise:
            def timeout_callback():
                self._active_calls.pop(counter, None)

            # Use the timeout from the handle, as it might have been modified
            promise = TimedPromise(handle._timeout, timeout_callback)

            # Register the handle in active calls
            self._active_calls[counter] = handle

            # Send the RPC call
            asyncio.create_task(self.pipe.write([1, 0, counter, method, list(args)]))
            return promise

        handle = RpcCallHandle(
            id_=counter,
            pipe=self.pipe,
            method=method,
            args=list(args),
            timeout=self._timeout,
            start_callback=start_callback,
        )
        return handle

    async def call_method(self, method: str, *args: Any) -> Any:
        return await self.create_call(method, *args).call().result()

    async def fire_method(self, method: str, *args: Any) -> None:
        counter = self._counter
        self._counter += 1
        await self.pipe.write([1, 1, counter, method, list(args)])

    async def emit(self, topic: str, message: Any) -> None:
        # Emit event: Protocol 3
        await self.pipe.write([3, 0, topic, message])

    def set_timeout(self, milliseconds: int) -> None:
        self._timeout = milliseconds

    def register_incoming_context(self, id_: int, context: RpcCallContext) -> None:
        self._incoming_contexts[id_] = context

    @abstractmethod
    def get_id(self) -> str:
        pass

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
    def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
        pass

    @abstractmethod
    async def on_event(self, topic: str, message: Any) -> None:
        pass

    @staticmethod
    def make_rpc_v1(
        pipe: EventPipe[Any, Any],
        id_: str,
        method_handler: Callable,
        event_handler: Optional[Callable] = None,
    ) -> "RpcV1":
        class ConcreteRpcV1(RpcV1):
            def get_id(self) -> str:
                return id_

            def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
                return method_handler(context, method, args)

            async def on_event(self, topic: str, message: Any) -> None:
                if event_handler:
                    if inspect.iscoroutinefunction(event_handler):
                        await event_handler(topic, message)
                    else:
                        event_handler(topic, message)

        return ConcreteRpcV1(pipe)

    @staticmethod
    def read_only_client(pipe: EventPipe[Any, Any]) -> "RpcV1":
        def method_handler(context: RpcCallContext, method: str, args: List[Any]) -> Any:
            raise Exception("Client Only Implementation")

        async def event_handler(topic: str, message: Any) -> None:
            print(f"Rpc Event dropped {topic} {message}", file=sys.stderr)

        return RpcV1.make_rpc_v1(pipe, "", method_handler, event_handler)
