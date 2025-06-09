"""
CBOR-RPC: An async-compatible CBOR-based RPC system
"""

from .event.emitter import AbstractEmitter
from .pipe.event_pipe import EventPipe
from .transformer import Transformer
from .promise import TimedPromise
from .rpc.rpc_base import RpcClient, RpcAuthorizedClient, RpcV1
from .rpc.rpc_server import RpcServer, RpcV1Server
from .pipe.server_base import Server
from .tcp import TcpPipe, TcpServer
from .transformer.json_transformer import JsonTransformer
from .pipe.pipe import Pipe

__all__ = [
    # Emitter
    'AbstractEmitter',

    # Pipe classes
    'EventPipe',
    'Pipe',
    'Transformer',

    # Promise
    'TimedPromise',

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
