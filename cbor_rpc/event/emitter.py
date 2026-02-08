from typing import Any, Dict, List, Callable
from abc import ABC, abstractmethod
import asyncio
import inspect
import traceback
import warnings


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

    def _run_background_task(self, coro: Callable[..., Any], *args: Any) -> None:
        async def runner():
            try:
                await coro(*args)
            except Exception as e:
                traceback.print_exc()
                warnings.warn(f"Background task error in handler: {e}", RuntimeWarning)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            warnings.warn("Background task skipped: no running event loop", RuntimeWarning)
            return

        loop.create_task(runner())

    def _emit(self, event_type: str, *args: Any) -> None:
        for sub in self._subscribers.get(event_type, []):
            try:
                if inspect.iscoroutinefunction(sub):
                    self._run_background_task(sub, *args)
                else:
                    sub(*args)
            except Exception as e:
                traceback.print_exc()
                warnings.warn(f"Synchronous error in handler: {e}", RuntimeWarning)

    async def _notify(self, event_type: str, *args: Any) -> None:
        """
        
        """
        for pipeline in self._pipelines.get(event_type, []):
            try:
                if inspect.iscoroutinefunction(pipeline):
                    await pipeline(*args)
                else:
                    pipeline(*args)
            except Exception as e:
                self._emit("error", e)
                raise e

        self._emit(event_type, *args)

    def on(self, event: str, handler: Callable) -> None:
        self._subscribers.setdefault(event, []).append(handler)

    def pipeline(self, event: str, handler: Callable) -> None:
        self._pipelines.setdefault(event, []).append(handler)
