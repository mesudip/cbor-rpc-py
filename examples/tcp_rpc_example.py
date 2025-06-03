"""
Example demonstrating how to use TcpDuplex with CBOR-RPC.
This example shows both client and server implementations using TCP transport.
"""

import asyncio
import json
from typing import Any, List
from cbor_rpc import TcpDuplex, TcpServer, RpcV1, RpcV1Server


class TcpRpcServer(RpcV1Server):
    """An RPC server that accepts TCP connections."""
    
    def __init__(self, host: str = '127.0.0.1', port: int = 0):
        super().__init__()
        self.host = host
        self.port = port
        self.tcp_server: TcpServer = None
    
    async def start(self):
        """Start the TCP server and begin accepting connections."""
        self.tcp_server = await TcpServer.create(self.host, self.port)
        
        async def on_connection(tcp_duplex: TcpDuplex):
            # Create a unique connection ID
            peer_info = tcp_duplex.get_peer_info()
            conn_id = f"{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
            
            # Add the TCP connection as an RPC client
            await self.add_connection(conn_id, tcp_duplex)
            print(f"New RPC client connected: {conn_id}")
        
        self.tcp_server.on_connection(on_connection)
        
        actual_host, actual_port = self.tcp_server.get_address()
        print(f"RPC server listening on {actual_host}:{actual_port}")
        return actual_host, actual_port
    
    async def stop(self):
        """Stop the TCP server."""
        if self.tcp_server:
            await self.tcp_server.close()
    
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle RPC method calls."""
        print(f"Method call from {connection_id}: {method}({args})")
        
        if method == "echo":
            return args[0] if args else None
        elif method == "add":
            return sum(args) if args else 0
        elif method == "get_server_info":
            return {
                "server": "TcpRpcServer",
                "connections": len(self.active_connections),
                "address": self.tcp_server.get_address() if self.tcp_server else None
            }
        else:
            raise Exception(f"Unknown method: {method}")
    
    async def validate_event_broadcast(self, connection_id: str, topic: str, message: Any) -> bool:
        """Validate whether an event should be broadcasted."""
        # Allow all events for this example
        return True


async def run_server():
    """Run the RPC server example."""
    server = TcpRpcServer()
    
    try:
        host, port = await server.start()
        print(f"Server started on {host}:{port}")
        
        # Keep the server running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        await server.stop()


async def run_client(host: str, port: int):
    """Run the RPC client example."""
    try:
        # Create TCP connection
        tcp_duplex = await TcpDuplex.create_connection(host, port)
        print(f"Connected to server at {host}:{port}")
        
        # Create RPC client
        def method_handler(method: str, args: List[Any]) -> Any:
            print(f"Server called method: {method}({args})")
            return f"Client response to {method}"
        
        async def event_handler(topic: str, message: Any) -> None:
            print(f"Received event: {topic} -> {message}")
        
        rpc_client = RpcV1.make_rpc_v1(tcp_duplex, "client", method_handler, event_handler)
        
        # Test method calls
        print("\n--- Testing RPC method calls ---")
        
        result = await rpc_client.call_method("echo", "Hello, Server!")
        print(f"Echo result: {result}")
        
        result = await rpc_client.call_method("add", 1, 2, 3, 4, 5)
        print(f"Add result: {result}")
        
        result = await rpc_client.call_method("get_server_info")
        print(f"Server info: {json.dumps(result, indent=2)}")
        
        # Test fire method (no response expected)
        await rpc_client.fire_method("echo", "Fire and forget message")
        print("Fired method (no response)")
        
        # Test event emission
        await rpc_client.emit("client_event", {"message": "Hello from client", "timestamp": "2024-01-01"})
        print("Emitted event")
        
        # Keep connection alive for a bit
        await asyncio.sleep(2)
        
    except Exception as e:
        print(f"Client error: {e}")
    finally:
        if 'tcp_duplex' in locals():
            await tcp_duplex.terminate()
        print("Client disconnected")


async def main():
    """Main example function."""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        await run_server()
    elif len(sys.argv) > 3 and sys.argv[1] == "client":
        host = sys.argv[2]
        port = int(sys.argv[3])
        await run_client(host, port)
    else:
        print("Usage:")
        print("  python tcp_rpc_example.py server")
        print("  python tcp_rpc_example.py client <host> <port>")
        print("\nRunning integrated example...")
        
        # Run integrated example
        server = TcpRpcServer()
        
        try:
            # Start server
            host, port = await server.start()
            
            # Give server time to start
            await asyncio.sleep(0.1)
            
            # Run client
            await run_client(host, port)
            
        finally:
            await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
