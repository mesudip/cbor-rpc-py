from typing import Any, Dict, List, Callable
from abc import ABC, abstractmethod
import asyncio
import inspect


class AbstractEmitter(ABC):
    def __init__(self):
        self._pipelines: Dict[str, List[Callable]] = {}
        self._subscribers: Dict[str, List[Callable]] = {}

    def unsubscribe(self, event: str, handler: Callable) -> None:
        if event in self._subscribers:
            self._subscribers[event] = [h for h in self._subscribers[event] if h != handler]

    def replace_on_handler(self, event_type: str, handler: Callable) -> None:
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

    def on(self, event: str, handler: Callable) -> None:
        self._subscribers.setdefault(event, []).append(handler)

    def pipeline(self, event: str, handler: Callable) -> None:
        self._pipelines.setdefault(event, []).append(handler)
