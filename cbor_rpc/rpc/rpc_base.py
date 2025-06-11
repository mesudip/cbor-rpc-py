from typing import Any, Dict, List, Optional, Callable
from abc import ABC, abstractmethod
import asyncio
import inspect
from ..pipe.event_pipe import EventPipe
from ..timed_promise import TimedPromise


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
    def with_client(self, connection_id: str, action: Callable) -> bool:
        pass

    @abstractmethod
    def set_timeout(self, milliseconds: int) -> None:
        pass

    @abstractmethod
    def is_active(self, connection_id: str) -> bool:
        pass
