from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
import asyncio
import inspect

# Generic type variables for Pipe
T1 = TypeVar('T1')
T2 = TypeVar('T2')

class AbstractEmitter(ABC):
    def __init__(self):
        self._pipelines: Dict[str, List[Callable]] = {}
        self._subscribers: Dict[str, List[Callable]] = {}

    def unsubscribe(self, event: str, handler: Callable) -> None:
        subscriber_list = self._subscribers.get(event, [])
        for i, sub in enumerate(subscriber_list):
            if sub == handler:
                subscriber_list.pop(i)
                break

    def replace_on_handler(self, event_type: str, handler: Callable) -> None:
        self._subscribers[event_type] = [handler]

    async def _emit(self, event_type: str, *args: Any) -> None:
        subscribers = self._subscribers.get(event_type, [])
        for sub in subscribers:
            if inspect.iscoroutinefunction(sub) or inspect.isasyncgenfunction(sub):
                await sub(*args)
            else:
                sub(*args)

    async def _notify(self, event_type: str, cb: Callable[[Optional[Any]], None], *args: Any) -> None:
        pipelines = self._pipelines.get(event_type, [])
        if pipelines:
            counter = len(pipelines)

            async def callback(err: Any = None) -> None:
                nonlocal counter
                if err:
                    counter = -1
                    await cb(err)
                else:
                    counter -= 1
                    if counter == 0:
                        await self._emit(event_type, *args)
                        await cb()

            for pipeline in pipelines:
                if inspect.iscoroutinefunction(pipeline) or inspect.isasyncgenfunction(pipeline):
                    try:
                        await pipeline(*args, callback)
                    except Exception as e:
                        await callback(e)
                else:
                    try:
                        pipeline(*args, callback)
                    except Exception as e:
                        await callback(e)
        else:
            await self._emit(event_type, *args)
            await cb()

    def on(self, event: str, handler: Callable) -> None:
        if event in self._subscribers:
            self._subscribers[event].append(handler)
        else:
            self._subscribers[event] = [handler]

    def pipeline(self, event: str, handler: Callable) -> None:
        if event in self._pipelines:
            self._pipelines[event].append(handler)
        else:
            self._pipelines[event] = [handler]

class Pipe(AbstractEmitter, Generic[T1, T2]):
    @abstractmethod
    async def write(self, chunk: T1, cb: Optional[Callable[[Optional[Exception]], None]] = None) -> bool:
        pass

    @abstractmethod
    async def terminate(self, *args: Any) -> None:
        pass

    def on(self, event: str, handler: Callable) -> None:
        if event == "data":
            async def data_handler(data: T2) -> None:
                if inspect.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler):
                    await handler(data)
                else:
                    handler(data)
            super().on(event, data_handler)
        elif event in ("error", "close"):
            async def error_handler(err: Exception) -> None:
                if inspect.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler):
                    await handler(err)
                else:
                    handler(err)
            super().on(event, error_handler)
        else:
            super().on(event, handler)

    def pipeline(self, event: str, handler: Callable[[T2, Callable[[Optional[Exception]], None]], None]) -> None:
        if event == "data":
            async def async_pipeline(data: T2, cb: Callable[[Optional[Exception]], None]) -> None:
                if inspect.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler):
                    await handler(data, cb)
                else:
                    handler(data, cb)
            super().pipeline(event, async_pipeline)
        else:
            super().pipeline(event, handler)

    @staticmethod
    def attach(source: 'Pipe[Any, Any]', destination: 'Pipe[Any, Any]') -> None:
        async def source_to_destination(chunk: Any) -> None:
            await destination.write(chunk)
        async def destination_to_source(chunk: Any) -> None:
            await source.write(chunk)
        async def close_handler(*args: Any) -> None:
            await destination._emit("close", *args)

        source.on("data", source_to_destination)
        destination.on("data", destination_to_source)
        source.on("close", close_handler)

    @staticmethod
    def make_pipe(writer: Callable[[T1, Optional[Callable[[Optional[Exception]], None]]], None],
                  terminator: Callable[..., None]) -> 'Pipe[T1, T2]':
        class ConcretePipe(Pipe[T1, T2]):
            async def write(self, chunk: T1, cb: Optional[Callable[[Optional[Exception]], None]] = None) -> bool:
                if inspect.iscoroutinefunction(writer) or inspect.isasyncgenfunction(writer):
                    await writer(chunk, cb)
                else:
                    writer(chunk, cb)
                return True

            async def terminate(self, *args: Any) -> None:
                if inspect.iscoroutinefunction(terminator) or inspect.isasyncgenfunction(terminator):
                    await terminator(*args)
                else:
                    terminator(*args)

        return ConcretePipe()




class DeferredPromise:
    def __init__(self, timeout_ms: int, timeout_cb: Callable[[], None], message: str = "Timeout on RPC call"):
        self._timeout_ms = timeout_ms
        self._timeout_cb = timeout_cb
        self._message = message
        self._loop = asyncio.get_event_loop()
        self._timeout_handle = None
        self._resolved = False
        
        self._future = self._loop.create_future()
        
        self._timeout_handle = self._loop.call_later(
            timeout_ms / 1000.0,
            self._timeout
        )

    @property
    def promise(self) -> asyncio.Future:
        return self._future

    async def resolve(self, result: Any) -> None:
        if self.clear_timeout() and not self._future.done():
            self._future.set_result(result)

    async def reject(self, error: Any) -> None:
        if self.clear_timeout() and not self._future.done():
            self._future.set_exception(Exception(error))

    def clear_timeout(self) -> bool:
        if self._timeout_handle:
            self._timeout_handle.cancel()
            self._timeout_handle = None
            return True
        return False

    def _timeout(self) -> None:
        self._future.set_exception(Exception({
            "timeout": True,
            "timeoutPeriod": self._timeout_ms,
            "message": self._message
        }))
        self._timeout_cb()

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

class RpcServer(ABC):
    @abstractmethod
    async def emit(self, connection_id: str, topic: str, message: Any) -> None:
        pass

    @abstractmethod
    async def broadcast(self, topic: str, message: Any) -> None:
        pass

    @abstractmethod
    async def call_method(self, connection_id: str, method: str, *args: Any) -> Any:
        pass

    @abstractmethod
    async def fire_method(self, connection_id: str, method: str, *args: Any) -> None:
        pass

    @abstractmethod
    async def disconnect(self, connection_id: str, reason: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def get_client(self, connection_id: str) -> Optional[RpcAuthorizedClient]:
        pass

    @abstractmethod
    def with_client(self, connection_id: str, action: Callable[[RpcAuthorizedClient], None]) -> bool:
        pass

    @abstractmethod
    def set_timeout(self, milliseconds: int) -> None:
        pass

    @abstractmethod
    def is_active(self, connection_id: str) -> bool:
        pass

class RpcV1(RpcClient):
    def __init__(self, pipe: Pipe[Any, Any]):
        self.pipe = pipe
        self._counter = 0
        self._promises: Dict[int, DeferredPromise] = {}
        self._timeout = 30000
        self._waiters: Dict[str, DeferredPromise] = {}

        async def on_data(data: List[Any]) -> None:
            if not isinstance(data, list) or len(data) != 5:
                print(f"RpcV1: Received unstructured message: expected array of length 5 got={data}")
                return

            version, direction, id_, method, params = data
            if version != 1:
                print(f"RpcV1: Received unsupported version of message {data}")
                return

            if direction < 2:
                async def on_error(error: Exception) -> None:
                    if direction == 0:
                        await self.pipe.write([1, 2, id_, False, str(error) or "Internal Exception"])
                    else:
                        print(f"Fired Method call error: method={method}, params={params}, error={error}")

                try:
                    result = self.handle_method_call(method, params)
                    if inspect.isawaitable(result):
                        try:
                            value = await result
                            if direction == 0:
                                await self.pipe.write([1, 2, id_, True, value])
                        except Exception as e:
                            await on_error(e)
                    elif direction == 0:
                        await self.pipe.write([1, 2, id_, True, result])
                except Exception as e:
                    await on_error(e)
            elif direction == 2:
                existing = self._promises.get(id_)
                if existing:
                    self._promises.pop(id_)
                    if method is True:
                        await existing.resolve(params)
                    else:
                        await existing.reject(params)
                else:
                    print(f"Received rpc reply for expired request id: {id_}, success={method}, data={params}")
            elif direction == 3:
                await self._on_event(method, params)
            else:
                print(f"RpcV1: Received invalid message direction {data}")

        self.pipe.on("data", on_data)

    async def call_method(self, method: str, *args: Any) -> Any:
        counter = self._counter
        self._counter += 1
        def_promise = DeferredPromise(self._timeout, lambda: self._promises.pop(counter, None))
        self._promises[counter] = def_promise
        await self.pipe.write([1, 0, counter, method, args])
        return await def_promise.promise

    async def fire_method(self, method: str, *args: Any) -> None:
        self._counter += 1
        await self.pipe.write([1, 1, self._counter - 1, method, args])

    async def emit(self, topic: str, args: Any) -> None:
        await self.pipe.write([1, 3, 0, topic, args])

    def set_timeout(self, milliseconds: int) -> None:
        self._timeout = milliseconds

    async def _on_event(self, method: str, message: Any) -> None:
        waiter = self._waiters.get(method)
        if waiter:
            self._waiters.pop(method)
            waiter.clear_timeout()
            await waiter.resolve(message)
            return
        await self.on_event(method, message)

    @abstractmethod
    def get_id(self) -> str:
        pass

    async def wait_next_event(self, topic: str, timeout_ms: Optional[int] = None) -> Any:
        if topic in self._waiters:
            raise Exception("Already waiting for event")
        waiter = DeferredPromise(
            timeout_ms or self._timeout,
            lambda: self._waiters.pop(topic, None),
            f"Timeout Waiting for Event on: {topic}"
        )
        self._waiters[topic] = waiter
        return await waiter.promise

    @abstractmethod
    def handle_method_call(self, method: str, args: List[Any]) -> Any:
        pass

    @abstractmethod
    async def on_event(self, topic: str, message: Any) -> None:
        pass

    @staticmethod
    def make_rpc_v1(
        pipe: Pipe[Any, Any],
        id_: str,
        method_handler: Callable[[str, List[Any]], Any],
        event_handler: Callable[[str, Any], None]
    ) -> 'RpcV1':
        class ConcreteRpcV1(RpcV1):
            def get_id(self) -> str:
                return id_

            def handle_method_call(self, method: str, args: List[Any]) -> Any:
                return method_handler(method, args)

            async def on_event(self, topic: str, message: Any) -> None:
                if inspect.iscoroutinefunction(event_handler) or inspect.isasyncgenfunction(event_handler):
                    await event_handler(topic, message)
                else:
                    event_handler(topic, message)

        return ConcreteRpcV1(pipe)

    @staticmethod
    def read_only_client(pipe: Pipe[Any, Any]) -> 'RpcV1':
        async def method_handler(method: str, args: List[Any]) -> Any:
            raise Exception("Client Only Implementation")

        async def event_handler(topic: str, message: Any) -> None:
            print(f"Rpc Event dropped {topic} {message}")

        return RpcV1.make_rpc_v1(pipe, '', method_handler, event_handler)
