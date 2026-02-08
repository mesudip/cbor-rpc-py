from .logging import RpcLogger


class RpcCallContext:
    def __init__(self, logger: RpcLogger):
        self.logger = logger
