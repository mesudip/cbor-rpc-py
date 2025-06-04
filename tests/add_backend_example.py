"""
Example script showing how to add new backend types to the test suite.
Run this to see how easy it is to extend the testing framework.
"""

import asyncio
from cbor_rpc import Pipe, JsonTransformer
from .helpers.backend_factory import BackendFactory
from .test_rpc_generic import SimpleTestRpcServer, JsonTestRpcServer


async def demonstrate_custom_backend():
    """Demonstrate creating and using a custom backend."""
    
    print("🚀 Creating custom backend...")
    
    # Example 1: Simple custom backend
    simple_backend = BackendFactory.create_simple_backend(
        name="demo-simple",
        description="Demo simple backend",
        server_class=SimpleTestRpcServer
    )
    
    server, client = await simple_backend.create_server_client_pair()
    
    try:
        print("✅ Testing simple backend...")
        result = await client.call_method("echo", "Hello Simple Backend!")
        print(f"   Result: {result}")
        
        result = await client.call_method("add", 10, 20, 30)
        print(f"   Math result: {result}")
        
    finally:
        await simple_backend.cleanup_backend(server, client)
    
    # Example 2: JSON transformer backend
    print("\n📄 Creating JSON backend...")
    
    json_backend = BackendFactory.create_transformer_backend(
        name="demo-json",
        description="Demo JSON backend",
        transformer_class=JsonTransformer,
        server_class=JsonTestRpcServer
    )
    
    server, client = await json_backend.create_server_client_pair()
    
    try:
        print("✅ Testing JSON backend...")
        result = await client.call_method("json_echo", "Hello JSON Backend!")
        print(f"   JSON result: {result}")
        
        complex_data = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
        result = await client.call_method("process_json_data", complex_data)
        print(f"   Complex data result: {result}")
        
    finally:
        await json_backend.cleanup_backend(server, client)
    
    print("\n🎉 Custom backend demonstration complete!")


if __name__ == "__main__":
    asyncio.run(demonstrate_custom_backend())
