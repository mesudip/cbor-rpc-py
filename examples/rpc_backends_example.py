"""
Example demonstrating how to use the same RPC code with different backend implementations.
"""

import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple
from cbor_rpc import (
    Pipe, SimplePipe, Duplex, TcpPipe, TcpServer,
    RpcV1, RpcV1Server
)


class Calculator:
    """A simple calculator service that can be exposed via RPC."""
    
    def add(self, *args):
        """Add numbers together."""
        return sum(args)
    
    def subtract(self, a, b):
        """Subtract b from a."""
        return a - b
    
    def multiply(self, *args):
        """Multiply numbers together."""
        result = 1
        for arg in args:
            result *= arg
        return result
    
    def divide(self, a, b):
        """Divide a by b."""
        if b == 0:
            raise ValueError("Cannot divide by zero")
        return a / b
    
    def calculate(self, expression):
        """Evaluate a simple mathematical expression."""
        # This is just for demonstration - never use eval in production code
        return eval(expression, {"__builtins__": {}})


class CalculatorRpcServer(RpcV1Server):
    """RPC server that exposes calculator functionality."""
    
    def __init__(self):
        super().__init__()
        self.calculator = Calculator()
        self._connections_info = {}
    
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle RPC method calls."""
        # Calculator methods
        if hasattr(self.calculator, method) and callable(getattr(self.calculator, method)):
            try:
                return getattr(self.calculator, method)(*args)
            except Exception as e:
                raise Exception(f"Calculator error: {str(e)}")
        
        # Server info methods
        if method == "get_server_info":
            return {
                "connections": len(self.active_connections),
                "connection_id": connection_id,
                "connection_info": self._connections_info.get(connection_id, {})
            }
        elif method == "set_client_info":
            if not args:
                raise Exception("Client info required")
            self._connections_info[connection_id] = args[0]
            return True
        
        raise Exception(f"Unknown method: {method}")
    
    async def validate_event_broadcast(self, connection_id: str, topic: str, message: Any) -> bool:
        """Validate whether an event should be broadcasted."""
        # Allow all events for this example
        return True


async def create_in_memory_pair() -> Tuple[CalculatorRpcServer, RpcV1]:
    """Create an in-memory server-client pair."""
    print("Creating in-memory RPC pair...")
    
    # Create server
    server = CalculatorRpcServer()
    
    # Create pipes for bidirectional communication
    client_to_server = SimplePipe()
    server_to_client = SimplePipe()
    
    # Connect the pipes
    Pipe.attach(client_to_server, server_to_client)
    
    # Create client pipe (duplex)
    client_pipe = Duplex()
    client_pipe.reader = server_to_client
    client_pipe.writer = client_to_server
    
    # Create server pipe (duplex)
    server_pipe = Duplex()
    server_pipe.reader = client_to_server
    server_pipe.writer = server_to_client
    
    # Add connection to server
    client_id = "in-memory-client"
    await server.add_connection(client_id, server_pipe)
    
    # Create client
    client = RpcV1.make_rpc_v1(
        client_pipe,
        client_id,
        lambda m, a: None,  # Client doesn't handle methods in this test
        lambda t, m: None   # Client doesn't handle events in this test
    )
    
    return server, client


async def create_tcp_pair() -> Tuple[CalculatorRpcServer, RpcV1]:
    """Create a TCP server-client pair."""
    print("Creating TCP RPC pair...")
    
    # Create server
    server = CalculatorRpcServer()
    
    # Create TCP server
    tcp_server = await TcpServer.create('127.0.0.1', 0)
    host, port = tcp_server.get_address()
    print(f"TCP server listening on {host}:{port}")
    
    # Set up connection handler
    client_ready = asyncio.Event()
    client_id = "tcp-client"
    
    async def on_connection(tcp_duplex: TcpPipe):
        await server.add_connection(client_id, tcp_duplex)
        client_ready.set()
    
    tcp_server.on_connection(on_connection)
    
    # Create client connection
    client_pipe = await TcpPipe.create_connection(host, port)
    
    # Wait for server to register the connection
    await client_ready.wait()
    
    # Create client
    client = RpcV1.make_rpc_v1(
        client_pipe,
        client_id,
        lambda m, a: None,  # Client doesn't handle methods in this test
        lambda t, m: None   # Client doesn't handle events in this test
    )
    
    # Store TCP server for cleanup
    server._tcp_server = tcp_server
    
    return server, client


async def run_calculator_demo(server, client):
    """Run the calculator demo with the given server and client."""
    try:
        # Set client info
        await client.call_method("set_client_info", {
            "name": "Demo Client",
            "version": "1.0.0"
        })
        
        # Get server info
        server_info = await client.call_method("get_server_info")
        print(f"Server info: {json.dumps(server_info, indent=2)}")
        
        # Test calculator methods
        print("\nTesting calculator methods:")
        
        result = await client.call_method("add", 1, 2, 3, 4, 5)
        print(f"1 + 2 + 3 + 4 + 5 = {result}")
        
        result = await client.call_method("subtract", 10, 5)
        print(f"10 - 5 = {result}")
        
        result = await client.call_method("multiply", 2, 3, 4)
        print(f"2 * 3 * 4 = {result}")
        
        result = await client.call_method("divide", 10, 2)
        print(f"10 / 2 = {result}")
        
        # Test error handling
        try:
            await client.call_method("divide", 10, 0)
        except Exception as e:
            print(f"Expected error: {e}")
        
        # Test expression evaluation
        result = await client.call_method("calculate", "2 * (3 + 4)")
        print(f"2 * (3 + 4) = {result}")
        
    except Exception as e:
        print(f"Error: {e}")


async def cleanup_server(server):
    """Clean up server resources."""
    # Close all connections
    for conn_id in list(server.active_connections.keys()):
        await server.disconnect(conn_id)
    
    # Close TCP server if it exists
    if hasattr(server, '_tcp_server') and server._tcp_server:
        await server._tcp_server.close()


async def main():
    """Run the example with both in-memory and TCP backends."""
    print("=== In-Memory RPC Example ===")
    server, client = await create_in_memory_pair()
    try:
        await run_calculator_demo(server, client)
    finally:
        await cleanup_server(server)
    
    print("\n=== TCP RPC Example ===")
    server, client = await create_tcp_pair()
    try:
        await run_calculator_demo(server, client)
    finally:
        await cleanup_server(server)


if __name__ == "__main__":
    asyncio.run(main())
