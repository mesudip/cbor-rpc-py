from typing import Any, Dict, List, Optional, Callable, TypeVar, Generic
from abc import ABC, abstractmethod
from ..pipe.event_pipe import EventPipe


class RpcInitClient(ABC):
    """
    Abstract base class for RPC clients.

    Note: In previous versions, this class was named `RpcClient`.
    """

    @abstractmethod
    async def call_method(self, method: str, *args: Any) -> Any:
        pass

    @abstractmethod
    async def fire_method(self, method: str, *args: Any) -> None:
        pass

    @abstractmethod
    def set_timeout(self, milliseconds: int) -> None:
        pass


class RpcClient(RpcInitClient):
    """
    RPC Client with ID.

    Note: In previous versions, this class was named `RpcAuthorizedClient`.
    """

    @abstractmethod
    def get_id(self) -> str:
        pass


T_ConnId = TypeVar("T_ConnId")


class RpcServer(ABC, Generic[T_ConnId]):
    @abstractmethod
    async def call_method(self, connection_id: T_ConnId, method: str, *args: Any) -> Any:
        pass

    @abstractmethod
    async def fire_method(self, connection_id: T_ConnId, method: str, *args: Any) -> None:
        pass

    @abstractmethod
    async def disconnect(self, connection_id: T_ConnId, reason: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def get_client(self, connection_id: T_ConnId) -> Optional[RpcClient]:
        pass

    @abstractmethod
    def with_client(self, connection_id: T_ConnId, action: Callable) -> bool:
        pass

    @abstractmethod
    def set_timeout(self, milliseconds: int) -> None:
        pass

    @abstractmethod
    def is_active(self, connection_id: T_ConnId) -> bool:
        pass
