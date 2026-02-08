import asyncio
import os
import re
import time

import asyncssh
import asyncssh.public_key
import docker
import pytest

from cbor_rpc.ssh.ssh_pipe import SshPipe, SshServer


@pytest.mark.asyncio
async def test_ssh_pipe_with_hello_world_emitter(ssh_server):
    print(f"\nAttempting SshPipe connection to {ssh_server.host}:{ssh_server.port}...")
    pipe = None
    try:
        emitter_command = "sh -c 'while true; do echo \"hello world\"; sleep 1; done'"
        pipe = await ssh_server.run_command(command=emitter_command)

        received_event = asyncio.Event()
        received_data = []

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_hello_world_emitter: Received data: {data!r}")
            received_data.append(data)
            if b"hello world" in data:
                received_event.set()

        pipe.pipeline("data", on_data_callback)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive 'hello world' within 10 seconds.")

        full_received_data = b"".join(received_data)
        assert b"hello world" in full_received_data

    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed.")


@pytest.mark.asyncio
async def test_ssh_pipe_with_echo_back_command(ssh_server):
    print(f"\nAttempting SshPipe connection to {ssh_server.host}:{ssh_server.port} for echo-back test...")
    pipe = None
    try:
        echo_back_command = "python3 /usr/local/bin/echo_back.py"
        pipe = await ssh_server.run_command(command=echo_back_command)

        received_data_chunks = []
        received_event = asyncio.Event()

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_echo_back_command: Received data chunk: {data!r}")
            received_data_chunks.append(data)
            received_event.set()

        pipe.pipeline("data", on_data_callback)

        test_message = b"This is a test message for echo back\n"
        print(f"Writing data to pipe: {test_message!r}")
        await pipe.write(test_message)
        await asyncio.sleep(1)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive any data within 10 seconds.")

        full_received_data = b"".join(received_data_chunks)
        assert (
            full_received_data.strip() == test_message.strip()
        ), f"Received data {full_received_data!r} should exactly match sent data {test_message!r}"
        print("Verification successful: Data echoed correctly by 'echo_back.py' script.")
        await pipe.write_eof()

    except asyncssh.Error as e:
        pytest.fail(f"SSH connection or command failed: {e}")
    except asyncio.TimeoutError:
        pytest.fail("SSH connection timed out.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed.")


@pytest.mark.asyncio
async def test_ssh_pipe_with_binary_data(ssh_server):
    print(f"\nAttempting SshPipe connection to {ssh_server.host}:{ssh_server.port} for binary data emitter test...")
    pipe = None
    try:
        emitter_binary_command = "python3 /usr/local/bin/binary_emitter.py"
        pipe = await ssh_server.run_command(command=emitter_binary_command)

        received_event = asyncio.Event()
        received_data_chunks = []
        expected_binary_pattern = b"\xde\xad\xbe\xef\x00\x01\x02\x03\x80\xff\x7f"

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_binary_data: Received data chunk: {data!r}")
            cleaned_data = data.replace(b"\r", b"").replace(b"\n", b"")
            received_data_chunks.append(cleaned_data)
            if expected_binary_pattern in cleaned_data:
                received_event.set()

        pipe.pipeline("data", on_data_callback)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive expected binary pattern within 10 seconds.")

        full_received_data = b"".join(received_data_chunks)
        print(f"Received total {len(full_received_data)} bytes. First 50 bytes: {full_received_data[:50]!r}")

        assert (
            expected_binary_pattern in full_received_data
        ), f"Expected binary pattern {expected_binary_pattern!r} not found in received data {full_received_data!r}"
        print("Verification successful: Binary data emitted and received correctly.")

    except asyncssh.Error as e:
        pytest.fail(f"SSH connection or command failed: {e}")
    except asyncio.TimeoutError:
        pytest.fail("SSH connection timed out.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed.")


@pytest.mark.asyncio
async def test_ssh_pipe_multiplexing(ssh_server):
    print(f"\nAttempting SshPipe connection to {ssh_server.host}:{ssh_server.port} with multiplexing...")

    # 1. Establish the main connection (pipe1)
    pipe1 = None
    pipe2 = None
    pipe3 = None
    try:
        # Use sleep to ensure the process stays alive long enough to test concurrency
        command1 = "sh -c 'sleep 2; echo Process 1'"
        pipe1 = await ssh_server.run_command(command=command1)

        received_event1 = asyncio.Event()
        received_data1 = []

        def on_data1(data):
            received_data1.append(data)
            if b"Process 1" in data:
                received_event1.set()

        pipe1.pipeline("data", on_data1)

        # 2. Create a second pipe using the same connection (pipe2)
        command2 = "sh -c 'sleep 2; echo Process 2'"
        pipe2 = await ssh_server.run_command(command=command2)

        received_event2 = asyncio.Event()
        received_data2 = []

        def on_data2(data):
            received_data2.append(data)
            if b"Process 2" in data:
                received_event2.set()

        pipe2.pipeline("data", on_data2)

        # Verify both channels are open concurrently (multiplexing check)
        # Without sleep, one might finish before the other starts.
        # Note: We check internal channel state
        assert not pipe1._ssh_channel.is_closing(), "Pipe 1 channel should be open"
        assert not pipe2._ssh_channel.is_closing(), "Pipe 2 channel should be open"
        print("Verified: Both SSH channels are open simultaneously.")

        # Wait for both (should take approx 2s total, not 4s)
        start_time = time.time()
        await asyncio.wait_for(asyncio.gather(received_event1.wait(), received_event2.wait()), timeout=10)
        duration = time.time() - start_time
        print(f"Multiplexed execution took {duration:.2f} seconds.")

        # Simple heuristic: if it was serial, it would be > 4s (2s + 2s).
        # Parallel is ~2s. Allow some buffer.
        assert duration < 3.8, "Processes should run in parallel"

        assert b"Process 1" in b"".join(received_data1)
        assert b"Process 2" in b"".join(received_data2)

        # 3. Verify they are on the same connection
        # Note: SSHClientProcess doesn't expose get_connection() directly in older asyncssh versions or standard API
        # But we know they were created from the same ssh_server instance which holds one connection
        assert not ssh_server._connection.is_closed()

        # 4. Terminate pipe2 (should NOT close connection)
        await pipe2.terminate()
        # Give a moment for cleanup
        await asyncio.sleep(0.1)
        # Connection should still be open
        assert not ssh_server._connection.is_closed()

        # 5. Connect a third pipe to verify connection is still usable
        command3 = "echo 'Process 3'"
        pipe3 = await ssh_server.run_command(command=command3)
        # We won't wait for output, just checking creation worked and needed to close it
        await pipe3.terminate()

    except Exception as e:
        pytest.fail(f"Multiplexing test failed: {e}")
    finally:
        # Clean up
        if pipe1:
            # Terminate pipe1 channel
            await pipe1.terminate()

        if pipe2:
            # Just in case it wasn't closed in test
            await pipe2.terminate()

        if pipe3:
            await pipe3.terminate()
