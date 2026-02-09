import asyncio
import os
import pytest
import asyncssh
from cbor_rpc.ssh.ssh_pipe import SshPipe, SshServer


@pytest.mark.asyncio
async def test_ssh_pipe_with_password_authentication(ssh_server_config):
    container, host, port, username, password, _ = ssh_server_config

    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} with password authentication...")
    server = None
    pipe = None
    try:
        server = await SshServer.connect(host=host, port=port, username=username, password=password, known_hosts=None)

        test_command = "echo 'Password auth successful!'"
        pipe = await server.run_command(command=test_command)

        received_event = asyncio.Event()
        received_data = []

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_password_authentication: Received data: {data!r}")
            received_data.append(data)
            if b"Password auth successful!" in data:
                received_event.set()

        pipe.pipeline("data", on_data_callback)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive expected output within 10 seconds for password authentication test.")

        full_received_data = b"".join(received_data)
        assert b"Password auth successful!" in full_received_data
        print("Password authentication successful.")

    except asyncssh.Error as e:
        pytest.fail(f"SSH connection or command failed with password authentication: {e}")
    except asyncio.TimeoutError:
        pytest.fail("SSH connection timed out with password authentication.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during password authentication test: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed for password authentication test.")
        if server:
            await server.close()


@pytest.mark.asyncio
async def test_ssh_pipe_with_plain_key_authentication(ssh_server_config, ssh_keys):
    container, host, port, username, _, unencrypted_private_key_content = ssh_server_config

    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} with plain key authentication...")

    server = None
    pipe = None
    try:
        server = await SshServer.connect(
            host=host, port=port, username=username, ssh_key_content=unencrypted_private_key_content, known_hosts=None
        )

        test_command = "echo 'Plain key auth successful!'"
        pipe = await server.run_command(command=test_command)

        received_event = asyncio.Event()
        received_data = []

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_plain_key_authentication: Received data: {data!r}")
            received_data.append(data)
            if b"Plain key auth successful!" in data:
                received_event.set()

        pipe.pipeline("data", on_data_callback)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive expected output within 10 seconds for plain key authentication test.")

        full_received_data = b"".join(received_data)
        assert b"Plain key auth successful!" in full_received_data
        print("Plain key authentication successful.")

    except asyncssh.Error as e:
        pytest.fail(f"SSH connection or command failed with plain key authentication: {e}")
    except asyncio.TimeoutError:
        pytest.fail("SSH connection timed out with plain key authentication.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during plain key authentication test: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed for plain key authentication test.")
        if server:
            await server.close()


@pytest.mark.asyncio
async def test_ssh_pipe_with_encrypted_key_authentication(ssh_server_config, ssh_keys):
    container, host, port, username, _, _ = ssh_server_config

    private_key_content = ssh_keys["encrypted_private"]
    passphrase = ssh_keys["passphrase"]

    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} with encrypted key authentication...")

    server = None
    pipe = None
    try:
        server = await SshServer.connect(
            host=host,
            port=port,
            username=username,
            ssh_key_content=private_key_content,
            ssh_key_passphrase=passphrase,
            known_hosts=None,
        )

        test_command = "echo 'Encrypted key auth successful!'"
        pipe = await server.run_command(command=test_command)

        received_event = asyncio.Event()
        received_data = []

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_encrypted_key_authentication: Received data: {data!r}")
            received_data.append(data)
            if b"Encrypted key auth successful!" in data:
                received_event.set()

        pipe.pipeline("data", on_data_callback)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive expected output within 10 seconds for encrypted key authentication test.")

        full_received_data = b"".join(received_data)
        assert b"Encrypted key auth successful!" in full_received_data
        print("Encrypted key authentication successful.")

    except asyncssh.Error as e:
        pytest.fail(f"SSH connection or command failed with encrypted key authentication: {e}")
    except asyncio.TimeoutError:
        pytest.fail("SSH connection timed out.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during encrypted key authentication test: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed for encrypted key authentication test.")
        if server:
            await server.close()
