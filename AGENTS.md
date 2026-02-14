# CBOR-RPC Project Documentation for LLMs

## Architecture Overview
The stack consists of: Transport (Pipe) -> Serialization (Transformer) -> Protocol (RPC).

**IMPORTANT**: All public components MUST be imported directly from `cbor_rpc`. DO NOT import from submodules (e.g., use `from cbor_rpc import TcpPipe`, NOT `from cbor_rpc.tcp.tcp import TcpPipe`).

## 1. Pipes (Transport)
All pipes implement `Pipe` (async base) or `EventPipe` (event-based).
Common methods: `write(chunk)`, `read(timeout)`, `terminate()`.

- **TcpPipe**: `await TcpPipe.create_connection(host, port)` (Client) or `await TcpPipe.create_server(host, port)` (Server).
- **StdioPipe**: `await StdioPipe.open()` (Use stdin/stdout to communicate) or `await StdioPipe.start_process(*args)` (Subprocess).
- **SshPipe**: Works over `asyncssh` channels.
- **EventPipe**: High-level wrapper emitting "data", "close", "error" events.

## 2. Transformers (Serialization)
Used to convert raw bytes from pipes into Python objects.
- **Implementations**: `CborStreamTransformer` (default), `JsonStreamTransformer`.
- **Usage**: `transformed_pipe = transformer.apply_transformer(raw_pipe)`.
- **Note**: Always use `*StreamTransformer` for TCP/Stdio to handle packet fragmentation.

## 3. RPC Layer
Provides high-level method calling over pipes.

- **`call_method(name, *args)`**: Async call. Wait for return value. Throws on timeout or remote error.
- **`fire_method(name, *args)`**: Async "fire and forget". No return value, no waiting.
- **`wait_next_event(topic)`**: Wait for a specific pulse/event from the other side.
- **`set_timeout(ms)`**: Set default timeout for all calls (default 30000ms).

### Client Construction
- **`RpcV1.read_only_client(pipe)`**: Use when you only need to call methods on the remote side.
```python
from cbor_rpc import TcpPipe, CborStreamTransformer, RpcV1

pipe = await TcpPipe.create_connection("localhost", 8080)
t_pipe = CborStreamTransformer().apply_transformer(pipe)
rpc = RpcV1.read_only_client(t_pipe)
result = await rpc.call_method("method_name", arg1, arg2)
```

### Server Usage
To run a server, subclass `RpcV1Server` for logic and `TcpServer` (or other) for transport. Override `accept` to apply the transformer and register the connection.

```python
from cbor_rpc import RpcV1Server, TcpServer, CborStreamTransformer

# 1. Implementation
class MyRpcApp(RpcV1Server):
    async def handle_method_call(self, conn_id, context, method, args):
        if method == "ping": return "pong"

# 2. Setup transport
class MyServer(TcpServer):
    def set_app(self, app): self.app = app
    
    async def accept(self, pipe):
        # Apply transformer to the raw connection pipe
        rpc_pipe = CborStreamTransformer().apply_transformer(pipe)
        # Register with the RPC app
        await self.app.add_connection(f"{pipe.get_peer_info()}", rpc_pipe)
        return True

# 3. Execution
app = MyRpcApp()
server = await MyServer.create(port=8080)
server.set_app(app)
```

## Tips
- **Timeouts**: Default 30s. Set via `rpc.set_timeout(ms)`.
- **Context**: `RpcCallContext` in handlers provides metadata/logging.
- **Logging**: Controlled via `cbor_rpc.RpcLogger`.
m