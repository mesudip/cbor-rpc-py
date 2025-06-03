from typing import Any, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
import asyncio
import inspect

# Generic type variables
T1 = TypeVar('T1')
T2 = TypeVar('T2')

class AbstractEmitter(ABC):
    def __init__(self):
        self._pipelines: Dict[str, List[callable]] = {}
        self._subscribers: Dict[str, List[callable]] = {}

    def unsubscribe(self, event: str, handler: callable) -> None:
        if event in self._subscribers:
            self._subscribers[event] = [h for h in self._subscribers[event] if h != handler]

    def replace_on_handler(self, event_type: str, handler: callable) -> None:
        self._subscribers[event_type] = [handler]

    async def _emit(self, event_type: str, *args: Any) -> None:
        for sub in self._subscribers.get(event_type, []):
            try:
                if inspect.iscoroutinefunction(sub):
                    await sub(*args)
                else:
                    sub(*args)
            except Exception:
                pass

    async def _notify(self, event_type: str, *args: Any) -> None:
        tasks = []
        for pipeline in self._pipelines.get(event_type, []):
            if inspect.iscoroutinefunction(pipeline):
                tasks.append(pipeline(*args))
            else:
                tasks.append(asyncio.to_thread(pipeline, *args))
        if tasks:
            await asyncio.gather(*tasks)
        await self._emit(event_type, *args)

    def on(self, event: str, handler: callable) -> None:
        self._subscribers.setdefault(event, []).append(handler)

    def pipeline(self, event: str, handler: callable) -> None:
        self._pipelines.setdefault(event, []).append(handler)

class Pipe(AbstractEmitter, Generic[T1, T2]):
    @abstractmethod
    async def write(self, chunk: T1) -> bool:
        pass

    @abstractmethod
    async def terminate(self, *args: Any) -> None:
        pass

    def on(self, event: str, handler: callable) -> None:
        super().on(event, handler)

    def pipeline(self, event: str, handler: callable) -> None:
        super().pipeline(event, handler)

    @staticmethod
    def attach(source: 'Pipe[Any, Any]', destination: 'Pipe[Any, Any]') -> None:
        source.on("data", lambda chunk: destination.write(chunk))
        destination.on("data", lambda chunk: source.write(chunk))
        source.on("close", lambda *args: destination._emit("close", *args))

    @staticmethod
    def make_pipe(writer: callable, terminator: callable) -> 'Pipe[T1, T2]':
        class ConcretePipe(Pipe[T1, T2]):
            async def write(self, chunk: T1) -> bool:
                if inspect.iscoroutinefunction(writer):
                    await writer(chunk)
                else:
                    writer(chunk)
                await self._notify("data",chunk)
                return True

            async def terminate(self, *args: Any) -> None:
                if inspect.iscoroutinefunction(terminator):
                    await terminator(*args)
                else:
                    terminator(*args)
        return ConcretePipe()
class DeferredPromise:
    def __init__(self, timeout_ms: int, message: str = "Timeout on RPC call"):
        self._future = asyncio.get_event_loop().create_future()
        self._timeout_handle = asyncio.get_event_loop().call_later(
            timeout_ms / 1000.0,
            lambda: self._future.set_exception(Exception({"timeout": True, "timeoutPeriod": timeout_ms, "message": message}))
        )

    @property
    def promise(self) -> asyncio.Future:
        return self._future

    async def resolve(self, result: Any) -> None:
        if self._timeout_handle:
            self._timeout_handle.cancel()
            if not self._future.done():
                self._future.set_result(result)

    async def reject(self, error: Any) -> None:
        if self._timeout_handle:
            self._timeout_handle.cancel()
            if not self._future.done():
                self._future.set_exception(Exception(error))

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
    def with_client(self, connection_id: str, action: callable) -> bool:
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

        async def resolve_result(result: Any) -> Any:
            """Recursively resolve coroutines or nested coroutines."""
            while asyncio.iscoroutine(result):
                result = await result
            return result

        def on_data(data: List[Any]) -> None:
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
                    async def send_response():
                        resolved_result = await resolve_result(result)
                        if direction == 0:
                            await self.pipe.write([1, 2, id_, True, resolved_result])
                    asyncio.create_task(send_response())
                except Exception as e:
                    if direction == 0:
                        asyncio.create_task(self.pipe.write([1, 2, id_, False, str(e)]))
                    else:
                        print(f"Fired method error: {method}, params={params}, error={e}")
            elif direction == 2:
                if promise := self._promises.pop(id_, None):
                    if method is True:
                        asyncio.create_task(promise.resolve(params))
                    else:
                        asyncio.create_task(promise.reject(params))
            elif direction == 3:
                asyncio.create_task(self._on_event(method, params))

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
    def make_rpc_v1(pipe: Pipe[Any, Any], id_: str, method_handler: callable, event_handler: callable) -> 'RpcV1':
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
        async def method_handler(method: str, args: List[Any]) -> Any:
            raise Exception("Client Only Implementation")

        async def event_handler(topic: str, message: Any) -> None:
            print(f"Rpc Event dropped {topic} {message}")
        return RpcV1.make_rpc_v1(pipe, '', method_handler, event_handler)


class RpcV1Server(RpcServer):
    def __init__(self):
        self.active_connections: Dict[str, RpcV1] = {}
        self.timeout = 30000

    async def add_connection(self, conn_id: str, rpc_client: Pipe[Any, Any]) -> None:
        client_rpc = RpcV1.make_rpc_v1(
            rpc_client,
            conn_id,
            lambda m, a: self.handle_method_call(conn_id, m, a),
            lambda t, d: self._handle_event(conn_id, t, d)
        )
        client_rpc.set_timeout(self.timeout)
        self.active_connections[conn_id] = client_rpc
        rpc_client.on("close", lambda *args: self.active_connections.pop(conn_id, None))

    async def disconnect(self, connection_id: str, reason: Optional[str] = None) -> None:
        if client := self.active_connections.pop(connection_id, None):
            await client.pipe.terminate(1000, reason or "Server terminated connection")

    def set_timeout(self, milliseconds: int) -> None:
        self.timeout = milliseconds

    def is_active(self, connection_id: str) -> bool:
        return connection_id in self.active_connections

    async def broadcast(self, topic: str, message: Any) -> None:
        await asyncio.gather(*(client.emit(topic, message) for client in self.active_connections.values()))

    def get_client(self, connection_id: str) -> Optional[RpcAuthorizedClient]:
        return self.active_connections.get(connection_id)

    async def call_method(self, connection_id: str, method: str, *args: Any) -> Any:
        if client := self.active_connections.get(connection_id):
            return await client.call_method(method, *args)
        raise Exception("Client is not active")

    async def emit(self, connection_id: str, topic: str, message: Any) -> None:
        if client := self.active_connections.get(connection_id):
            await client.emit(topic, message)
        else:
            raise Exception("Client is not active")

    async def _handle_event(self, connection_id: str, topic: str, message: Any) -> None:
        if await self.validate_event_broadcast(connection_id, topic, message):
            await asyncio.gather(*(
                client.emit(topic, message)
                for key, client in self.active_connections.items()
                if key != connection_id
            ))

    async def fire_method(self, connection_id: str, method: str, *args: Any) -> None:
        if client := self.active_connections.get(connection_id):
            await client.fire_method(method, *args)
        else:
            raise Exception("Client is not active")

    @abstractmethod
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        pass

    @abstractmethod
    async def validate_event_broadcast(self, connection_id: str, topic: str, message: Any) -> bool:
        pass

    def with_client(self, connection_id: str, action: callable) -> bool:
        if client := self.active_connections.get(connection_id):
            action(client)
            return True
        return False

class SimplePipe(Pipe[T1, T1], Generic[T1]):
    def __init__(self):
        super().__init__()

    async def write(self, chunk: T1) -> bool:
        await self._notify("data", chunk)
        return True

    async def terminate(self, *args: Any) -> None:
        await self._emit("close", *args)

class Duplex(Pipe[T1, T2], Generic[T1, T2]):
    def __init__(self):
        super().__init__()
        self.reader: Pipe[T1, Any] = SimplePipe()
        self.writer: Pipe[Any, T2] = SimplePipe()
        self.reader.on("error", lambda err: self._emit("error", err))
        self.writer.on("error", lambda err: self._emit("error", err))
        self.reader.pipeline("data", lambda chunk, *args: self._notify("data", chunk))

    async def write(self, chunk: Any) -> bool:
        await self.writer.write(chunk)
        return True

    async def terminate(self, *args: Any) -> None:
        await asyncio.gather(self.reader.terminate(*args), self.writer.terminate(*args))
