"""
CBOR-RPC: An async-compatible CBOR-based RPC system
"""

from .event.emitter import AbstractEmitter
from .pipe.event_pipe import EventPipe
from .transformer import Transformer
from .timed_promise import TimedPromise
from .rpc.rpc_base import RpcClient, RpcAuthorizedClient,RpcServer
from .rpc.rpc_v1 import RpcV1
from .rpc.rpc_server import RpcV1Server
from .rpc.server_base import Server
from .tcp import TcpPipe, TcpServer
from .transformer.json_transformer import JsonTransformer
from .pipe.pipe import Pipe

__all__ = [
    # Promise
    'TimedPromise',

    # Emitter
    'AbstractEmitter',


    # Pipe abstract  classes
    'EventPipe',
    'Pipe',

    # Server abstract classes
    'Server',

    # Rpc abstract classes 
    'RpcClient',
    'RpcAuthorizedClient',
    'RpcServer',

    # Rpc base implementation
    'RpcV1',
    'RpcV1Server',

    # TCP classes
    'TcpPipe',
    'TcpServer',

    # Transformers
    'Transformer',
    'JsonTransformer',
]

__version__ = "0.1.0"
