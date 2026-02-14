cbor-rpc
========
[![codecov](https://codecov.io/github/mesudip/cbor-rpc-py/graph/badge.svg)](https://codecov.io/github/mesudip/cbor-rpc-py)

A lightweight, event-based RPC framework for Python using CBOR (optionally JSON) over various transport layers.

## Table of Contents
- [RPC System](#rpc-system)
  - [Capabilities](#capabilities)
  - [Creating a Server](#creating-a-server)
  - [Creating a Client](#creating-a-client)
- [Pipes and Event Pipes](#pipes-and-event-pipes)
- [Transformers](#transformers)
- [High-level Pipes](#high-level-pipes)

---

## RPC System

The RPC system is built on top of `EventPipe` and `Transformers`.

### Capabilities
- **Bidirectional**: Both sides can act as server and client.
- **Logs & Progress**: Real-time streaming of log messages and progress updates during a rpc call.
- **Events**: Broadcast and listen to topics.
- **Async/Await**: Native support for Python's `asyncio`.
- **Method Cancellation**: Long-running calls can be cancelled by the caller.

### Creating a Server
Extend `RpcV1Server` and implement `handle_method_call`.

```python
import asyncio
from cbor_rpc import RpcV1Server, TcpServer, CborStreamTransformer

class MyService(RpcV1Server):
    def __init__(self, tcp_server: TcpServer):
        super().__init__()
        # Configure server to handle new connections
        tcp_server.on("connection", self.on_connection)

    async def on_connection(self, tcp_pipe):
        print(f"New connection from {tcp_pipe.get_peer_info()}")
        rpc_pipe = CborStreamTransformer().apply_transformer(tcp_pipe)
        
        # Add connection to RPC system
        conn_id = str(tcp_pipe.get_peer_info())
        await self.add_connection(conn_id, rpc_pipe)

    async def handle_method_call(self, connection_id, context, method, args):
        if method == "add":
            return args[0] + args[1]
        raise Exception("Unknown method")

async def run_server():
    server = await TcpServer.create("0.0.0.0", 9000)
    service = MyService(server)
    print("Server running on 9000...")
    await asyncio.Future()  # block forever

# asyncio.run(run_server())
```

### Creating a Client
Use the `RpcV1` class to wrap an object-oriented pipe.

```python
import asyncio
from typing import Any, List
from cbor_rpc import RpcV1, RpcCallContext, TcpPipe, CborStreamTransformer

# 1. Define Client with Methods (Bidirectional)
class MyClient(RpcV1):
    def get_id(self) -> str:
        return "client-node"

    async def handle_method_call(self, context: RpcCallContext, method: str, args: List[Any]) -> Any:
        # Handle calls FROM the server
        if method == "ping":
            return "pong"
        raise Exception(f"Unknown method {method}")

    async def on_event(self, topic: str, message: Any) -> None:
        print(f"Event: {topic} -> {message}")

async def run_client():
    # 2. Connect via TCP
    tcp_pipe = await TcpPipe.create_connection("localhost", 9000)
    
    # 3. Apply CBOR Transformer
    cbor_pipe = CborStreamTransformer().apply_transformer(tcp_pipe)
    
    # 4. Instantiate Custom Client
    client = MyClient(cbor_pipe)
    
    # 5. Make calls (Client -> Server)
    result = await client.call_method("add", 5, 10)
    print(f"5 + 10 = {result}")

    # Call with logs and progress
    handle = client.create_call("long_task")
    handle.on_log(lambda level, msg: print(f"LOG: {msg}"))
    handle.on_progress(lambda val, meta: print(f"Progress: {val}%"))

    # await handle.call()

# asyncio.run(run_client())
```

---

## Pipes and Event Pipes

The core of `cbor-rpc` is the **Pipe** abstraction. Unlike traditional unidirectional pipes, a **Pipe** in this framework represents a **duplex connection**. It allows you to both:
- **Write** messages to the remote side.
- **Read** replies or incoming messages from the remote side.

This abstraction provides a consistent interface for bidirectional communication across different transport layers (TCP streams, SSH channels, Stdio, etc.).

### Basic Usage (`EventPipe`)
Most "real-world" pipes (TCP, SSH, Stdio) are `EventPipe`s. They are event-driven, meaning you register listeners for incoming data instead of polling.

#### Consuming Data
There are two ways to listen for data:
2.  **`pipeline("data", handler)`**: Used for serial processing. Handlers are awaited in order. If a pipeline handler throws an error, it stops the chain and emits an `"error"`.
1.  **`on("data", handler)`**: Simple pub/sub. The handler is called whenever data arrives. If it's a coroutine, it's run in the background.

```python
# Simple listener
pipe.on("data", lambda chunk: print(f"Received {len(chunk)} bytes"))

# Serial processing (e.g., for transformers)
async def process_data(chunk):
    # This is awaited before the next chunk is processed
    await do_something(chunk)

pipe.pipeline("data", process_data)
```

#### Sending Data
Use the `write()` method to send data through the pipe.

```python
await pipe.write(b"Request data")
```

### Converting a `Pipe` to an `EventPipe`
If you have a raw `Pipe` (which uses `read()`/`write()`), you can convert it to an `EventPipe` using `make_event_based()`:

```python
event_pipe = raw_pipe.make_event_based()
event_pipe.on("data", handle_incoming)
```

## Transformers

Transformers allow you to convert raw data (typically `bytes`) into high-level Python objects and vice-versa.

### Available Transformers
The library comes with two built-in transformer types:
- **`JsonTransformer`** / **`JsonStreamTransformer`**: Encodes/decodes JSON data.
- **`CborTransformer`** / **`CborStreamTransformer`**: Encodes/decodes CBOR data (ideal for binary efficiency).

### Stream Support
It is important to note that **not all transformers support streams**.
- A standard **`Transformer`** (like `JsonTransformer`) expects a complete message in each chunk. It maps 1 input -> 1 output.
- A **Stream Transformer** (like `JsonStreamTransformer`) is designed to handle fragmented data (e.g., from TCP). It buffers incoming bytes until a complete message can be decoded.

**When using TCP or SSH pipes, you almost always want to use a `...StreamTransformer`.**

### Using Transformers
You can wrap a byte-based pipe with a transformer to create an object-based pipe.

```python
from cbor_rpc.transformer.json_transformer import JsonStreamTransformer

# Wrap a raw pipe
object_pipe = JsonStreamTransformer().apply_transformer(raw_pipe)

# Now 'data' events emit Python objects
object_pipe.on("data", lambda obj: print(f"Received object: {obj}"))
await object_pipe.write({"method": "hello", "params": []})
```

### Making a Custom Transformer
To create your own transformer, subclass `Transformer` (for single packets) or `AsyncTransformer` (for streams):

```python
from cbor_rpc.transformer.base import Transformer

class MyUpperTransformer(Transformer[str, str]):
    def encode(self, data: str) -> str:
        return data.upper()

    def decode(self, data: str) -> str:
        return data.lower()
```
---

## High-level Pipes

`cbor-rpc` provides several ready-to-use pipe implementations for different transport layers.

### TCP Pipe (`TcpPipe`)
Used for network communication over TCP.

```python
from cbor_rpc.tcp import TcpPipe

# Client
pipe = await TcpPipe.create_connection("localhost", 8000)

# Server
from cbor_rpc.tcp import TcpServer
class MyServer(TcpServer):
    async def accept(self, pipe: TcpPipe) -> bool:
        print("New connection!")
        return True

server = await MyServer.create("0.0.0.0", 8000)
```

### SSH Pipe (`SshPipe`)
Tunneling through SSH using `asyncssh`.

```python
from cbor_rpc.ssh import SshPipe
# Used typically to run a command on a remote host and communicate with it
```

### Stdio Pipe (`StdioPipe`)
Communicate with subprocesses via stdin/stdout.

```python
from cbor_rpc.stdio import StdioPipe

# Start a subprocess
pipe = await StdioPipe.start_process("python3", "worker.py")
```

---

Development
-----------
Enable local git hooks to auto-format on commit:

```
pre-commit install
```