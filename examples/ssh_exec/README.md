# SSH Execution Examples

This directory contains examples of how to use `SshPipe` to execute processes, retrieve results, and stream logs over SSH.

## Prerequisites

- An SSH server available to connect to.
- For `rpc_client.py`: The `cbor-rpc` library must be installed on the remote machine, or the `examples.fs_rpc.filesystem_server` (or compatible server) must be runnable via the specified command.

## Examples

### 1. Simple Command Execution (`simple_exec.py`)

This script connects to a remote host, executes a command, and streams the output (stdout) back to the local console. It can be used to run one-off scripts, check system status, or stream logs.

**Usage:**

```bash
# List files on remote
python simple_exec.py --host <HOST> --user <USER> --password <PASS> "ls -la"

# Stream a log file (follow)
python simple_exec.py --host <HOST> --user <USER> --password <PASS> "tail -f /var/log/syslog"

# Run a python script remotely
python simple_exec.py --host <HOST> --user <USER> --password <PASS> "python3 -c 'import sys; print(sys.version)'"
```

### 2. RPC over SSH (`rpc_client.py`)

This script demonstrates how to run the CBOR-RPC protocol over an SSH tunnel. It starts a server process on the remote machine and interacts with it using `RpcInitClient`.

**Setup:**

You need to ensure the remote machine has the `cbor-rpc` library installed and the `stdio_rpc_server.py` script available.

```bash
# Copy the server script to the remote machine
scp examples/ssh_exec/stdio_rpc_server.py user@host:~/stdio_rpc_server.py
```

**Usage:**

```bash
# Connect and run RPC
python rpc_client.py --host <HOST> --user <USER> --password <PASS> \
    --remote-cmd "python3 ~/stdio_rpc_server.py"
```

The server script (`stdio_rpc_server.py`) uses standard input/output to communicate, which `SshPipe` connects to.
