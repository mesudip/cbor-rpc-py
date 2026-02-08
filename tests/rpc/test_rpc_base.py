from typing import Any, Optional

import pytest

from cbor_rpc.rpc.rpc_base import RpcClient, RpcServer


class DummyClient(RpcClient):
    async def call_method(self, method: str, *args: Any) -> Any:
        return await RpcClient.call_method(self, method, *args)

    async def fire_method(self, method: str, *args: Any) -> None:
        return await RpcClient.fire_method(self, method, *args)

    def set_timeout(self, milliseconds: int) -> None:
        return RpcClient.set_timeout(self, milliseconds)


class DummyServerBase(RpcServer):
    async def call_method(self, connection_id: str, method: str, *args: Any) -> Any:
        return await RpcServer.call_method(self, connection_id, method, *args)

    async def fire_method(self, connection_id: str, method: str, *args: Any) -> None:
        return await RpcServer.fire_method(self, connection_id, method, *args)

    async def disconnect(self, connection_id: str, reason: Optional[str] = None) -> None:
        return await RpcServer.disconnect(self, connection_id, reason)

    def get_client(self, connection_id: str):
        return RpcServer.get_client(self, connection_id)

    def with_client(self, connection_id: str, action):
        return RpcServer.with_client(self, connection_id, action)

    def set_timeout(self, milliseconds: int) -> None:
        return RpcServer.set_timeout(self, milliseconds)

    def is_active(self, connection_id: str) -> bool:
        return RpcServer.is_active(self, connection_id)


@pytest.mark.asyncio
async def test_rpc_base_methods_execute():
    client = DummyClient()
    assert await client.call_method("m") is None
    assert await client.fire_method("m") is None
    assert client.set_timeout(1) is None

    server = DummyServerBase()
    assert await server.call_method("id", "m") is None
    assert await server.fire_method("id", "m") is None
    assert await server.disconnect("id") is None
    assert server.get_client("id") is None
    assert server.with_client("id", lambda _c: None) is None
    assert server.set_timeout(1) is None
    assert server.is_active("id") is None
