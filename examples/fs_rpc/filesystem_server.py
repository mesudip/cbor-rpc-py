import os
from typing import List, Optional, Any
from cbor_rpc import RpcV1Server

class FilesystemRpcServer(RpcV1Server):
    async def validate_event_broadcast(self, connection_id, topic, message):
        return False
    async def handle_method_call(self, connection_id: str, method: str, args: List[Any]) -> Any:
        if method == "list_files":
            return self.list_files(*args)
        elif method == "read_file":
            return self.read_file(*args)
        elif method == "create_file":
            return self.create_file(*args)
        elif method == "delete_file":
            return self.delete_file(*args)
        elif method == "rename_file":
            return self.rename_file(*args)
        else:
            raise Exception(f"Unknown method: {method}")

    def list_files(self, directory: str) -> List[str]:
        """Lists files and directories in the given path."""
        try:
            return os.listdir(directory)
        except Exception as e:
            return f"Error listing files: {str(e)}"

    def read_file(self, path: str, chunk_size: int = 4096, offset: int = 0) -> bytes:
        """Reads a file in chunks."""
        try:
            with open(path, 'rb') as f:
                f.seek(offset)
                return f.read(chunk_size)
        except Exception as e:
            return f"Error reading file: {str(e)}".encode()

    def create_file(self, path: str, content: Optional[bytes] = None) -> bool:
        """Creates a file with optional initial content."""
        try:
            with open(path, 'wb') as f:
                if content:
                    f.write(content)
            return True
        except Exception as e:
            print(f"Error creating file: {str(e)}")
            return False

    def delete_file(self, path: str) -> bool:
        """Deletes a file."""
        try:
            os.remove(path)
            return True
        except Exception as e:
            print(f"Error deleting file: {str(e)}")
            return False

    def rename_file(self, src: str, dest: str) -> bool:
        """Renames/moves a file."""
        try:
            os.rename(src, dest)
            return True
        except Exception as e:
            print(f"Error renaming file: {str(e)}")
            return False

if __name__ == "__main__":
    import asyncio
    from cbor_rpc.tcp import TcpPipe, TcpServer
    from cbor_rpc.transformer.json_transformer import JsonTransformer
    async def main():
        rpc_id=1
        # Create a TCP server that handles connections, using JsonTransformer for RPC messages
        tcp_server = await TcpServer.create("localhost", 8000)
        print("Server running on port 8000")

        # Set up event handlers for new connections
        async def handle_connection( rpc_pipe):
            server = FilesystemRpcServer()
            await server.add_connection(str(rpc_id), rpc_pipe)

        tcp_server.on_connection(handle_connection)

        # Just run until manually stopped
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await tcp_server.stop()

    asyncio.run(main())
