"""
CBOR-RPC: An async-compatible CBOR-based RPC system
"""

from .event import AbstractEmitter
from .pipe import EventPipe,Pipe
from .timed_promise import TimedPromise
from .rpc import RpcClient, RpcAuthorizedClient,RpcServer,RpcV1,RpcV1Server,Server
from .tcp import TcpPipe, TcpServer
from .transformer import JsonTransformer,Transformer

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
