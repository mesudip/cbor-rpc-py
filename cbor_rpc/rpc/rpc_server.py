from typing import Any, Dict, List, Optional, Callable
from abc import ABC, abstractmethod
import asyncio

from cbor_rpc.rpc.server_base import Server
from .rpc_base import RpcClient, RpcAuthorizedClient, RpcServer
from .rpc_v1 import RpcV1
from cbor_rpc.pipe.event_pipe import EventPipe


class RpcV1Server(RpcServer):
    def __init__(self, server: Server):
        self.active_connections: Dict[str, RpcV1] = {}
        self.timeout = 30000

    async def add_connection(self, conn_id: str, rpc_client: EventPipe[Any, Any]) -> None:
        def method_handler(method: str, args: List[Any]) -> Any:
            return self.handle_method_call(conn_id, method, args)

        async def event_handler(topic: str, data: Any) -> None:
            await self._handle_event(conn_id, topic, data)

        client_rpc = RpcV1.make_rpc_v1(rpc_client, conn_id, method_handler, event_handler)
        client_rpc.set_timeout(self.timeout)
        self.active_connections[conn_id] = client_rpc

        # Set up cleanup on close
        async def cleanup(*args):
            self.active_connections.pop(conn_id, None)

        rpc_client.on("close", cleanup)

    async def disconnect(self, connection_id: str, reason: Optional[str] = None) -> None:
        client = self.active_connections.pop(connection_id, None)
        if client:
            print("RpcV1Server: Disconnecting client:", connection_id)
            await client.pipe.terminate(1000, reason or "Server terminated connection")

    def set_timeout(self, milliseconds: int) -> None:
        self.timeout = milliseconds

    def is_active(self, connection_id: str) -> bool:
        return connection_id in self.active_connections

    async def broadcast(self, topic: str, message: Any) -> None:
        tasks = []
        for client in self.active_connections.values():
            tasks.append(client.emit(topic, message))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_client(self, connection_id: str) -> Optional[RpcAuthorizedClient]:
        return self.active_connections.get(connection_id)

    async def call_method(self, connection_id: str, method: str, *args: Any) -> Any:
        client = self.active_connections.get(connection_id)
        if client:
            return await client.call_method(method, *args)
        raise Exception("Client is not active")

    async def emit(self, connection_id: str, topic: str, message: Any) -> None:
        client = self.active_connections.get(connection_id)
        if client:
            await client.emit(topic, message)
        else:
            raise Exception("Client is not active")

    async def _handle_event(self, connection_id: str, topic: str, message: Any) -> None:
        if await self.validate_event_broadcast(connection_id, topic, message):
            tasks = []
            for key, client in self.active_connections.items():
                if key != connection_id:
                    tasks.append(client.emit(topic, message))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def fire_method(self, connection_id: str, method: str, *args: Any) -> None:
        client = self.active_connections.get(connection_id)
        if client:
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
        client = self.active_connections.get(connection_id)
        if client:
            action(client)
            return True
        return False
