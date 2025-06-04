"""
Example demonstrating JsonTransformer usage with different server types.
"""

import asyncio
import json
from typing import Any, List
from cbor_rpc import (
    Server, TcpServer, TcpPipe, JsonTransformer,
    RpcV1, RpcV1Server, Pipe
)


class JsonRpcServer(RpcV1Server):
    """RPC server that works with JSON-transformed data."""
    
    def __init__(self):
        super().__init__()
        self._message_log = []
    
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        """Handle RPC method calls."""
        self._message_log.append(f"JSON RPC call: {method} from {connection_id}")
        
        if method == "echo":
            return {"echo": args[0] if args else None, "timestamp": "2024-01-01"}
        elif method == "add_numbers":
            return {"result": sum(args), "operation": "addition"}
        elif method == "get_log":
            return {"log": self._message_log.copy()}
        elif method == "process_data":
            # Process complex JSON data
            data = args[0] if args else {}
            return {
                "processed": True,
                "input_keys": list(data.keys()) if isinstance(data, dict) else [],
                "input_type": type(data).__name__
            }
        else:
            raise Exception(f"Unknown method: {method}")
    
    async def validate_event_broadcast(self, connection_id: str, topic: str, message: Any) -> bool:
        """Validate event broadcasts."""
        self._message_log.append(f"JSON event: {topic} from {connection_id}")
        return True


async def demonstrate_json_over_tcp():
    """Demonstrate JSON RPC over TCP with JsonTransformer."""
    print("=== JSON RPC over TCP Example ===")
    
    # Create TCP server
    tcp_server: Server[TcpPipe] = await TcpServer.create('127.0.0.1', 0)
    rpc_server = JsonRpcServer()
    
    async def on_tcp_connection(tcp_pipe: TcpPipe):
        # Wrap TCP pipe with JSON transformer
        json_pipe = JsonTransformer(tcp_pipe)
        
        # Create connection ID
        peer_info = tcp_pipe.get_peer_info()
        conn_id = f"json-tcp-{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
        
        # Add JSON-transformed connection to RPC server
        await rpc_server.add_connection(conn_id, json_pipe)
        print(f"New JSON RPC client connected: {conn_id}")
    
    tcp_server.on_connection(on_tcp_connection)
    
    try:
        # Get server address
        host, port = tcp_server.get_address()
        print(f"JSON RPC server listening on {host}:{port}")
        
        # Create client with JSON transformer
        tcp_client_pipe = await TcpPipe.create_connection(host, port)
        json_client_pipe = JsonTransformer(tcp_client_pipe)
        
        # Create RPC client
        rpc_client = RpcV1.make_rpc_v1(
            json_client_pipe,
            "json-client",
            lambda m, a: {"client_response": f"Handled {m}"},
            lambda t, m: print(f"Client received event: {t} -> {m}")
        )
        
        await asyncio.sleep(0.1)  # Let connection establish
        
        # Test JSON RPC calls
        print("\n--- Testing JSON RPC calls ---")
        
        # Simple echo
        result = await rpc_client.call_method("echo", "Hello JSON World!")
        print(f"Echo result: {json.dumps(result, indent=2)}")
        
        # Math operation
        result = await rpc_client.call_method("add_numbers", 10, 20, 30)
        print(f"Add result: {json.dumps(result, indent=2)}")
        
        # Complex data processing
        complex_data = {
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ],
            "metadata": {
                "version": "1.0",
                "timestamp": "2024-01-01T00:00:00Z"
            }
        }
        result = await rpc_client.call_method("process_data", complex_data)
        print(f"Process data result: {json.dumps(result, indent=2)}")
        
        # Get server log
        result = await rpc_client.call_method("get_log")
        print(f"Server log: {json.dumps(result, indent=2)}")
        
        # Test event emission
        await rpc_client.emit("user_action", {
            "action": "login",
            "user": "alice",
            "timestamp": "2024-01-01T12:00:00Z"
        })
        print("Emitted JSON event")
        
        await tcp_client_pipe.terminate()
        
    finally:
        await tcp_server.stop()
        print("JSON RPC server stopped")


async def demonstrate_json_transformer_pair():
    """Demonstrate direct JsonTransformer pair communication."""
    print("\n=== Direct JSON Transformer Pair Example ===")
    
    # Create JSON transformer pair
    json_transformer1, json_transformer2 = JsonTransformer.create_pair()
    
    # Set up data handlers
    transformer1_received = []
    transformer2_received = []
    
    json_transformer1.on("data", lambda data: transformer1_received.append(data))
    json_transformer2.on("data", lambda data: transformer2_received.append(data))
    
    # Test bidirectional JSON communication
    test_data_1 = {
        "message": "Hello from transformer 1",
        "data": [1, 2, 3, {"nested": True}]
    }
    
    test_data_2 = {
        "response": "Hello from transformer 2",
        "status": "success",
        "metadata": {"processed_at": "2024-01-01"}
    }
    
    await json_transformer1.write(test_data_1)
    await json_transformer2.write(test_data_2)
    await asyncio.sleep(0.1)
    
    print("Transformer 1 sent:", json.dumps(test_data_1, indent=2))
    print("Transformer 2 received:", json.dumps(transformer2_received[0], indent=2))
    print()
    print("Transformer 2 sent:", json.dumps(test_data_2, indent=2))
    print("Transformer 1 received:", json.dumps(transformer1_received[0], indent=2))
    
    # Verify data integrity
    assert transformer2_received[0] == test_data_1
    assert transformer1_received[0] == test_data_2
    print("\n✅ JSON data integrity verified!")


async def demonstrate_json_error_handling():
    """Demonstrate JSON transformer error handling."""
    print("\n=== JSON Error Handling Example ===")
    
    pipe1, pipe2 = Pipe.create_pair()
    json_transformer = JsonTransformer(pipe1)
    
    errors = []
    json_transformer.on("error", lambda err: errors.append(str(err)))
    
    # Test encoding error
    class NonSerializable:
        def __repr__(self):
            return "NonSerializable()"
    
    print("Testing encoding error...")
    result = await json_transformer.write(NonSerializable())
    print(f"Write result: {result}")
    await asyncio.sleep(0.01)
    
    if errors:
        print(f"Encoding error caught: {errors[-1]}")
    
    # Test decoding error
    print("\nTesting decoding error...")
    await pipe2.write(b'{"invalid": json syntax}')
    await asyncio.sleep(0.01)
    
    if len(errors) > 1:
        print(f"Decoding error caught: {errors[-1]}")
    
    # Test recovery
    print("\nTesting error recovery...")
    valid_data = {"message": "Recovery successful"}
    await pipe2.write(json.dumps(valid_data).encode('utf-8'))
    
    received_data = []
    json_transformer.on("data", lambda data: received_data.append(data))
    await asyncio.sleep(0.01)
    
    if received_data:
        print(f"Recovery successful: {json.dumps(received_data[0], indent=2)}")
    
    print(f"\nTotal errors handled: {len(errors)}")


async def main():
    """Run all JSON transformer demonstrations."""
    await demonstrate_json_over_tcp()
    await demonstrate_json_transformer_pair()
    await demonstrate_json_error_handling()


if __name__ == "__main__":
    asyncio.run(main())
