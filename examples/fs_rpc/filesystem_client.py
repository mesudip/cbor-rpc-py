import asyncio
from cbor_rpc import RpcV1
from cbor_rpc.tcp import TcpPipe
from cbor_rpc.transformer.json_transformer import JsonTransformer
from cbor_rpc.pipe.event_pipe import (
    EventPipe,
)  # Keep this import for clarity, though not directly instantiated


async def main():
    # Connect to the RPC server
    tcp_pipe = await TcpPipe.create_connection("localhost", 8000)  # Use port 8002

    # Create a JSON transformer and attach the TCP pipe to it
    json_transformer = JsonTransformer(tcp_pipe)

    # The RpcV1 client needs a pipe that it can write to and read from.
    # The json_transformer now handles both incoming and outgoing data.
    rpc_client = RpcV1.read_only_client(json_transformer)

    # Example usage of filesystem RPC methods

    # List files in current directory
    files = await rpc_client.call_method("list_files", ".")
    print("Files in current directory:", files)

    # Create a test file
    create_success = await rpc_client.call_method("create_file", "test.txt")
    print("File creation successful:", create_success)

    # Read the test file (should be empty)
    content = await rpc_client.call_method("read_file", "test.txt")
    print("File content:", content.decode())

    # Write to the test file
    write_success = await rpc_client.call_method("create_file", "test.txt", b"Hello, world!")
    print("Write successful:", write_success)

    # Read the updated file
    content = await rpc_client.call_method("read_file", "test.txt")
    print("Updated file content:", content.decode())

    # Rename the file
    rename_success = await rpc_client.call_method("rename_file", "test.txt", "renamed_test.txt")
    print("Rename successful:", rename_success)

    # Delete the renamed file
    delete_success = await rpc_client.call_method("delete_file", "renamed_test.txt")
    print("Delete successful:", delete_success)


if __name__ == "__main__":
    asyncio.run(main())
