"""
Helper module for creating custom RPC backend configurations.
This makes it easy to add new backend types for testing.
"""

import uuid
from typing import Any, Callable, List, Optional, Tuple
from cbor_rpc import Pipe, RpcV1
from ..test_rpc_generic import BaseTestRpcServer, RpcBackendConfig


class BackendFactory:
    """Factory class for creating custom RPC backend configurations."""
    
    @staticmethod
    def create_simple_backend(
        name: str,
        description: str,
        server_class: type = None,
        pipe_factory: Callable[[], Tuple[Pipe, Pipe]] = None,
        cleanup_func: Optional[Callable] = None
    ) -> RpcBackendConfig:
        """
        Create a simple backend configuration.
        
        Args:
            name: Backend name
            description: Backend description  
            server_class: Server class to use (defaults to BaseTestRpcServer)
            pipe_factory: Function that creates (server_pipe, client_pipe) tuple
            cleanup_func: Optional cleanup function
            
        Returns:
            A custom RpcBackendConfig instance
        """
        if server_class is None:
            from ..test_rpc_generic import SimpleTestRpcServer
            server_class = SimpleTestRpcServer
            
        if pipe_factory is None:
            pipe_factory = Pipe.create_pair
        
        class CustomSimpleBackend(RpcBackendConfig):
            def __init__(self):
                super().__init__(name=name, description=description)
            
            async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
                server = server_class()
                server_pipe, client_pipe = pipe_factory()
                
                client_id = f"{name}-client-{uuid.uuid4()}"
                await server.add_connection(client_id, server_pipe)
                
                def client_method_handler(method: str, args: List[Any]) -> Any:
                    return f"{name} client handled {method}"
                
                async def client_event_handler(topic: str, message: Any) -> None:
                    pass
                
                client = RpcV1.make_rpc_v1(
                    client_pipe, client_id, client_method_handler, client_event_handler
                )
                
                return server, client
            
            async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
                if cleanup_func:
                    await cleanup_func(server, client)
                await server.cleanup()
        
        return CustomSimpleBackend()
    
    @staticmethod
    def create_transformer_backend(
        name: str,
        description: str,
        transformer_class: type,
        server_class: type = None,
        base_pipe_factory: Callable[[], Tuple[Pipe, Pipe]] = None,
        cleanup_func: Optional[Callable] = None,
        **transformer_kwargs
    ) -> RpcBackendConfig:
        """
        Create a backend configuration with data transformation.
        
        Args:
            name: Backend name
            description: Backend description
            transformer_class: Transformer class to wrap pipes with
            server_class: Server class to use
            base_pipe_factory: Function that creates base pipes
            cleanup_func: Optional cleanup function
            **transformer_kwargs: Additional arguments for transformer constructor
            
        Returns:
            A custom RpcBackendConfig instance with transformation
        """
        if server_class is None:
            from ..test_rpc_generic import SimpleTestRpcServer
            server_class = SimpleTestRpcServer
            
        if base_pipe_factory is None:
            base_pipe_factory = Pipe.create_pair
        
        class CustomTransformerBackend(RpcBackendConfig):
            def __init__(self):
                super().__init__(name=name, description=description)
            
            async def create_server_client_pair(self) -> Tuple[BaseTestRpcServer, RpcV1]:
                server = server_class()
                
                # Create base pipes
                raw_server_pipe, raw_client_pipe = base_pipe_factory()
                
                # Wrap with transformers
                server_pipe = transformer_class(raw_server_pipe, **transformer_kwargs)
                client_pipe = transformer_class(raw_client_pipe, **transformer_kwargs)
                
                client_id = f"{name}-client-{uuid.uuid4()}"
                await server.add_connection(client_id, server_pipe)
                
                def client_method_handler(method: str, args: List[Any]) -> Any:
                    return {
                        "client_response": f"{name} client handled {method}",
                        "transformer": transformer_class.__name__
                    }
                
                async def client_event_handler(topic: str, message: Any) -> None:
                    pass
                
                client = RpcV1.make_rpc_v1(
                    client_pipe, client_id, client_method_handler, client_event_handler
                )
                
                return server, client
            
            async def cleanup_backend(self, server: BaseTestRpcServer, client: RpcV1):
                if cleanup_func:
                    await cleanup_func(server, client)
                await server.cleanup()
        
        return CustomTransformerBackend()


# Example usage functions for common backend types
def create_json_backend(name: str = "custom-json", description: str = "Custom JSON backend"):
    """Create a JSON transformer backend."""
    from cbor_rpc import JsonTransformer
    from ..test_rpc_generic import JsonTestRpcServer
    
    return BackendFactory.create_transformer_backend(
        name=name,
        description=description,
        transformer_class=JsonTransformer,
        server_class=JsonTestRpcServer
    )


def create_tcp_backend(name: str = "custom-tcp", description: str = "Custom TCP backend"):
    """Create a TCP-based backend."""
    from cbor_rpc import TcpPipe, TcpServer
    
    async def tcp_pipe_factory():
        return await TcpPipe.create_pair()
    
    async def tcp_cleanup(server, client):
        if hasattr(server, '_tcp_server') and server._tcp_server:
            await server._tcp_server.close()
    
    return BackendFactory.create_simple_backend(
        name=name,
        description=description,
        pipe_factory=tcp_pipe_factory,
        cleanup_func=tcp_cleanup
    )


# Template for adding new transformer types
def create_custom_transformer_backend(transformer_class, name_suffix="custom"):
    """Template for creating backends with custom transformers."""
    return BackendFactory.create_transformer_backend(
        name=f"in-memory-{name_suffix}",
        description=f"In-memory communication with {transformer_class.__name__}",
        transformer_class=transformer_class
    )
