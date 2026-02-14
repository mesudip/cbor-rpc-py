from typing import Any, Dict, List, Optional, Callable
from abc import ABC, abstractmethod

from cbor_rpc.rpc.server_base import Server
from .rpc_base import RpcClient, RpcServer
from .rpc_v1 import RpcV1
from .context import RpcCallContext
from cbor_rpc.pipe.event_pipe import EventPipe


class RpcV1Server(RpcServer):
    def __init__(self):
        self.active_connections: Dict[str, EventPipe[Any, Any]] = {}
        self.rpc_clients: Dict[str, RpcV1] = {}
        self.timeout = 30000

    async def add_connection(self, conn_id: str, rpc_client: EventPipe[Any, Any]) -> None:
        def method_handler(context: RpcCallContext, method: str, args: List[Any]) -> Any:
            return self.handle_method_call(conn_id, context, method, args)

        client_rpc = RpcV1.make_rpc_v1(rpc_client, conn_id, method_handler)
        client_rpc.set_timeout(self.timeout)

        self.active_connections[conn_id] = rpc_client
        self.rpc_clients[conn_id] = client_rpc

        # Set up cleanup on close
        async def cleanup(*args):
            self.active_connections.pop(conn_id, None)
            self.rpc_clients.pop(conn_id, None)

        rpc_client.on("close", cleanup)

    async def disconnect(self, connection_id: str, reason: Optional[str] = None) -> None:
        base = self.active_connections.pop(connection_id, None)
        self.rpc_clients.pop(connection_id, None)
        if base:
            print("RpcV1Server: Disconnecting client:", connection_id)
            await base.terminate(1000, reason or "Server terminated connection")

    def set_timeout(self, milliseconds: int) -> None:
        self.timeout = milliseconds

    def is_active(self, connection_id: str) -> bool:
        return connection_id in self.rpc_clients

    def get_client(self, connection_id: str) -> Optional[RpcClient]:
        return self.rpc_clients.get(connection_id)

    async def call_method(self, connection_id: str, method: str, *args: Any) -> Any:
        client = self.rpc_clients.get(connection_id)
        if client:
            return await client.call_method(method, *args)
        raise Exception("Client is not active")

    async def fire_method(self, connection_id: str, method: str, *args: Any) -> None:
        client = self.rpc_clients.get(connection_id)
        if client:
            await client.fire_method(method, *args)
        else:
            raise Exception("Client is not active")

    @abstractmethod
    async def handle_method_call(
        self,
        connection_id: str,
        context: RpcCallContext,
        method: str,
        args: List[Any],
    ) -> Any:
        pass

    def with_client(self, connection_id: str, action: Callable) -> bool:
        client = self.rpc_clients.get(connection_id)
        if client:
            action(client)
            return True
        return False
