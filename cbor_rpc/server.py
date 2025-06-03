from typing import Any, Dict, List, Optional, Callable
from abc import ABC, abstractmethod
import asyncio
from .client import RpcClient, RpcAuthorizedClient, RpcV1
from .pipe import Pipe


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
    def with_client(self, connection_id: str, action: Callable) -> bool:
        pass

    @abstractmethod
    def set_timeout(self, milliseconds: int) -> None:
        pass

    @abstractmethod
    def is_active(self, connection_id: str) -> bool:
        pass


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

    def with_client(self, connection_id: str, action: Callable) -> bool:
        if client := self.active_connections.get(connection_id):
            action(client)
            return True
        return False
