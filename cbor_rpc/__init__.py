"""
CBOR-RPC: An async-compatible CBOR-based RPC system
"""

from .emitter import AbstractEmitter
from .async_pipe import Pipe
from .transformer import Transformer
from .promise import DeferredPromise
from .client import RpcClient, RpcAuthorizedClient, RpcV1
from .server import RpcServer, RpcV1Server
from .server_base import Server
from .tcp import TcpPipe, TcpServer
from .json_transformer import JsonTransformer
from .sync_pipe import SyncPipe

__all__ = [
    # Emitter
    'AbstractEmitter',

    # Pipe classes
    'Pipe',
    'SyncPipe',
    'Transformer',

    # Promise
    'DeferredPromise',

    # Client classes
    'RpcClient',
    'RpcAuthorizedClient',
    'RpcV1',

    # Server classes
    'Server',
    'RpcServer',
    'RpcV1Server',

    # TCP classes
    'TcpPipe',
    'TcpServer',

    # Transformers
    'JsonTransformer',
]

__version__ = "0.1.0"
