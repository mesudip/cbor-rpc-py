from typing import Any, Callable


class RpcLogger:
    def __init__(
        self,
        send_log: Callable[[int, int, Any, Any], None],
        ref_proto: int,
        ref_id: Callable[[], Any],
    ):
        self._send_log = send_log
        self._ref_proto = ref_proto
        self._ref_id = ref_id

    def log(self, content: Any) -> None:
        self._send_log(3, self._ref_proto, self._ref_id(), content)

    def warn(self, content: Any) -> None:
        self._send_log(2, self._ref_proto, self._ref_id(), content)

    def crit(self, content: Any) -> None:
        self._send_log(1, self._ref_proto, self._ref_id(), content)

    def verbose(self, content: Any) -> None:
        self._send_log(4, self._ref_proto, self._ref_id(), content)

    def debug(self, content: Any) -> None:
        self._send_log(5, self._ref_proto, self._ref_id(), content)
