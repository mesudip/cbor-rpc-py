
from typing import Any, Callable, List


class Pipe:
    def __init__(self):
        self._on_data_callback = None

    def on(self, event: str, callback: Callable[[List[Any]], None]) -> None:
        if event == "data":
            self._on_data_callback = callback

    def write(self, data: List[Any]) -> None:
        pass  # Placeholder for actual implementation
