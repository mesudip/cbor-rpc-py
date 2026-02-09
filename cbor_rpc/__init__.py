"""
CBOR-RPC: An async-compatible CBOR-based RPC system
"""

from .event import AbstractEmitter
from .pipe import EventPipe, Pipe
from .timed_promise import TimedPromise
from .tcp import TcpPipe, TcpServer
from .transformer import CborStreamTransformer, CborTransformer, JsonTransformer, Transformer
from .rpc import (
    RpcClient,
    RpcAuthorizedClient,
    RpcServer,
    RpcV1,
    RpcV1Server,
    Server,
    RpcCallContext,
    RpcLogger,
)

__all__ = [
    # Promise
    "TimedPromise",
    # Emitter
    "AbstractEmitter",
    # Pipe abstract  classes
    "EventPipe",
    "Pipe",
    # Server abstract classes
    "Server",
    # Rpc abstract classes
    "RpcClient",
    "RpcAuthorizedClient",
    "RpcServer",
    # Rpc base implementation
    "RpcV1",
    "RpcV1Server",
    # Rpc high level
    "RpcCallContext",
    "RpcLogger",  # TCP classes
    "TcpPipe",
    "TcpServer",
    # Transformers
    "Transformer",
    "JsonTransformer",
    "CborTransformer",
    "CborStreamTransformer",
]

__version__ = "0.1.0"
