"""
CBOR-RPC: An async-compatible CBOR-based RPC system
"""

from .event import AbstractEmitter
from .pipe import AioPipe, EventPipe, Pipe
from .timed_promise import TimedPromise
from .tcp import TcpPipe, TcpServer
from .stdio import StdioPipe
from .ssh import SshPipe, SshServer
from .transformer import (
    AsyncTransformer,
    CborStreamTransformer,
    CborTransformer,
    EventTransformerPipe,
    JsonStreamTransformer,
    JsonTransformer,
    Transformer,
    TransformerPipe,
)
from .rpc import (
    RpcInitClient,
    RpcClient,
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
    "AioPipe",
    "EventPipe",
    "Pipe",
    # Server abstract classes
    "Server",
    # Rpc abstract classes
    "RpcInitClient",
    "RpcClient",
    "RpcServer",
    # Rpc base implementation
    "RpcV1",
    "RpcV1Server",
    # Rpc high level
    "RpcCallContext",
    "RpcLogger",
    # Pipe implementations
    "TcpPipe",
    "TcpServer",
    "StdioPipe",
    "SshPipe",
    "SshServer",
    # Transformers
    "Transformer",
    "AsyncTransformer",
    "JsonTransformer",
    "JsonStreamTransformer",
    "CborTransformer",
    "CborStreamTransformer",
    "TransformerPipe",
    "EventTransformerPipe",
]

__version__ = "0.1.0"
