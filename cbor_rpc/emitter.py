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
        if event in self._pipelines:
            self._pipelines[event] = [h for h in self._pipelines[event] if h != handler]

    def replace_on_handler(self, event_type: str, handler: Callable) -> None:
        self._subscribers[event_type] = [handler]

    async def _emit(self, event_type: str, *args: Any) -> None:
        for sub in self._subscribers.get(event_type, []):
            try:
                if inspect.iscoroutinefunction(sub):
                    await sub(*args)
                else:
                    sub(*args)
            except Exception as e:
                # We should log the exception but not propagate it
                print(f"Error in event handler: {e}")

    async def _notify(self, event_type: str, *args: Any) -> None:
        tasks = []

        for pipeline in self._pipelines.get(event_type, []):
            if inspect.iscoroutinefunction(pipeline):
                task = asyncio.create_task(pipeline(*args))
                tasks.append(task)
            else:
                try:
                    pipeline(*args)
                except Exception as e:
                    await self._emit("error", e)
                    raise e

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    await self._emit("error", result)
                    raise result

        await self._emit(event_type, *args)

    def on(self, event: str, handler: Callable) -> None:
        self._subscribers.setdefault(event, []).append(handler)

    def pipeline(self, event: str, handler: Callable) -> None:
        self._pipelines.setdefault(event, []).append(handler)
