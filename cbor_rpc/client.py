from typing import Any, Dict, List, Optional, Callable
from abc import ABC, abstractmethod
import asyncio
import inspect
from .pipe import Pipe
from .promise import DeferredPromise


class RpcClient(ABC):
    @abstractmethod
    async def emit(self, topic: str, message: Any) -> None:
        pass

    @abstractmethod
    async def call_method(self, method: str, *args: Any) -> Any:
        pass

    @abstractmethod
    async def fire_method(self, method: str, *args: Any) -> None:
        pass

    @abstractmethod
    def set_timeout(self, milliseconds: int) -> None:
        pass


class RpcAuthorizedClient(RpcClient):
    @abstractmethod
    def get_id(self) -> str:
        pass


class RpcV1(RpcClient):
    def __init__(self, pipe: Pipe[Any, Any]):
        self.pipe = pipe
        self._counter = 0
        self._promises: Dict[int, DeferredPromise] = {}
        self._timeout = 30000
        self._waiters: Dict[str, DeferredPromise] = {}

        async def resolve_result(result: Any) -> Any:
            """Recursively resolve coroutines or nested coroutines."""
            while asyncio.iscoroutine(result):
                result = await result
            return result

        async def on_data(data: List[Any]) -> None:
            if not isinstance(data, list) or len(data) != 5:
                print(f"RpcV1: Invalid message format: {data}")
                return

            version, direction, id_, method, params = data
            if version != 1:
                print(f"RpcV1: Unsupported version: {data}")
                return

            if direction < 2:
                try:
                    # Call the method and get the result
                    result = self.handle_method_call(method, params)
                    # Resolve coroutines asynchronously
                    try:
                        resolved_result = await resolve_result(result)
                        if direction == 0:
                            await self.pipe.write([1, 2, id_, True, resolved_result])
                    except Exception as e:
                        if direction == 0:
                            await self.pipe.write([1, 2, id_, False, str(e)])
                        else:
                            print(f"Fired method error: {method}, params={params}, error={e}")
                except Exception as e:
                    if direction == 0:
                        await self.pipe.write([1, 2, id_, False, str(e)])
                    else:
                        print(f"Fired method error: {method}, params={params}, error={e}")
            elif direction == 2:
                if promise := self._promises.pop(id_, None):
                    if method is True:
                        await promise.resolve(params)
                    else:
                        await promise.reject(params)
            elif direction == 3:
                await self._on_event(method, params)

        self.pipe.on("data", on_data)

    async def call_method(self, method: str, *args: Any) -> Any:
        counter = self._counter
        self._counter += 1
        promise = DeferredPromise(self._timeout)
        self._promises[counter] = promise
        await self.pipe.write([1, 0, counter, method, args])
        return await promise.promise

    async def fire_method(self, method: str, *args: Any) -> None:
        self._counter += 1
        await self.pipe.write([1, 1, self._counter - 1, method, args])

    async def emit(self, topic: str, args: Any) -> None:
        await self.pipe.write([1, 3, 0, topic, args])

    def set_timeout(self, milliseconds: int) -> None:
        self._timeout = milliseconds

    async def _on_event(self, method: str, message: Any) -> None:
        if waiter := self._waiters.pop(method, None):
            await waiter.resolve(message)
        else:
            await self.on_event(method, message)

    @abstractmethod
    def get_id(self) -> str:
        pass

    async def wait_next_event(self, topic: str, timeout_ms: Optional[int] = None) -> Any:
        if topic in self._waiters:
            raise Exception("Already waiting for event")
        waiter = DeferredPromise(timeout_ms or self._timeout)
        self._waiters[topic] = waiter
        return await waiter.promise

    @abstractmethod
    def handle_method_call(self, method: str, args: List[Any]) -> Any:
        pass

    @abstractmethod
    async def on_event(self, topic: str, message: Any) -> None:
        pass

    @staticmethod
    def make_rpc_v1(pipe: Pipe[Any, Any], id_: str, method_handler: Callable, event_handler: Callable) -> 'RpcV1':
        class ConcreteRpcV1(RpcV1):
            def get_id(self) -> str:
                return id_

            def handle_method_call(self, method: str, args: List[Any]) -> Any:
                return method_handler(method, args)

            async def on_event(self, topic: str, message: Any) -> None:
                if inspect.iscoroutinefunction(event_handler):
                    await event_handler(topic, message)
                else:
                    event_handler(topic, message)
        return ConcreteRpcV1(pipe)

    @staticmethod
    def read_only_client(pipe: Pipe[Any, Any]) -> 'RpcV1':
        def method_handler(method: str, args: List[Any]) -> Any:
            raise Exception("Client Only Implementation")

        async def event_handler(topic: str, message: Any) -> None:
            print(f"Rpc Event dropped {topic} {message}")
        return RpcV1.make_rpc_v1(pipe, '', method_handler, event_handler)
