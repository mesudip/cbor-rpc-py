import asyncio
import pytest
import docker
import time
import asyncssh
import os
import re
import asyncssh.public_key

from cbor_rpc.ssh.ssh_pipe import SshPipe

# Define a test user and password for the SSHD container
TEST_SSH_USER = "testuser"
TEST_SSH_PASSWORD = "testpassword"
SSHD_IMAGE_NAME = "cbor-rpc-py-sshd-python"  # Custom image name
SSHD_CONTAINER_NAME = "test-sshd-container"
SSHD_DOCKERFILE_PATH = "./tests/docker/sshd-python"  # Path to the Dockerfile


@pytest.fixture(scope="session")
def ssh_keys():
    """Generates SSH keys (plain and encrypted) and a passphrase for testing."""
    private_key_obj = asyncssh.generate_private_key("ssh-rsa")
    passphrase = "test_passphrase"

    return {
        "unencrypted_private": private_key_obj.export_private_key().decode(),
        "unencrypted_public": private_key_obj.export_public_key().decode(),
        "encrypted_private": private_key_obj.export_private_key(passphrase=passphrase).decode(),
        "encrypted_public": private_key_obj.export_public_key().decode(),
        "passphrase": passphrase,
    }


@pytest.fixture(scope="session")
def docker_client():
    """Provides a Docker client instance."""
    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture(scope="session")
def test_network(docker_client: docker.DockerClient):
    """Provides a Docker network for containers to communicate."""
    network_name = "test-ssh-network"
    try:
        network = docker_client.networks.get(network_name)
        network.remove()  # Clean up existing network if it exists
    except docker.errors.NotFound:
        pass

    network = docker_client.networks.create(network_name, driver="bridge")
    yield network
    network.remove()


@pytest.fixture(scope="module")  # Changed scope to module as requested
async def ssh_container_combined_auth(docker_client: docker.DockerClient, test_network, docker_host_ip, ssh_keys):
    container_name = "ssh-test-container-combined-auth"
    ssh_user = TEST_SSH_USER
    ssh_password = TEST_SSH_PASSWORD
    public_key = ssh_keys["unencrypted_public"]  # Use the unencrypted public key

    # Ensure previous container is stopped and removed
    try:
        existing_container = docker_client.containers.get(container_name)
        print(f"Found existing container '{container_name}'. Stopping and removing...")
        existing_container.stop()
        existing_container.remove()
        print(f"Removed existing container '{container_name}'.")
    except docker.errors.NotFound:
        print(f"No existing container '{container_name}' found. Proceeding.")
    except Exception as e:
        print(f"Error cleaning up existing container: {e}")

    # Build the custom Docker image
    print(f"\nBuilding Docker image '{SSHD_IMAGE_NAME}' from '{SSHD_DOCKERFILE_PATH}'...")
    try:
        docker_client.images.build(
            path=SSHD_DOCKERFILE_PATH,
            tag=SSHD_IMAGE_NAME,
            rm=True,  # Remove intermediate containers
        )
        print(f"Docker image '{SSHD_IMAGE_NAME}' built successfully.")
    except docker.errors.BuildError as e:
        print(f"Failed to build Docker image: {e}")
        raise RuntimeError(f"Failed to build Docker image '{SSHD_IMAGE_NAME}'.")

    print(f"\nStarting {SSHD_IMAGE_NAME} container with combined auth...")
    container = None
    try:
        container = docker_client.containers.run(
            SSHD_IMAGE_NAME,  # Use the custom image name
            detach=True,
            ports={"2222/tcp": None},  # Map container SSH port to a random host port
            network=test_network.name,
            name=container_name,
            environment={
                "PUID": "1000",
                "PGID": "1000",
                "TZ": "Etc/UTC",
                "PASSWORD_ACCESS": "true",  # Password access enabled
                "USER_NAME": ssh_user,
                "USER_PASSWORD": ssh_password,
                "PUBLIC_KEY": public_key,  # Add one public key
            },
            restart_policy={"Name": "no"},
        )

        container.reload()
        host_port = None
        for _ in range(30):  # Wait up to 30 seconds for port mapping
            container.reload()
            if "2222/tcp" in container.ports and container.ports["2222/tcp"]:
                host_port = container.ports["2222/tcp"][0]["HostPort"]
                break
            time.sleep(1)

        if host_port is None:
            raise RuntimeError("Failed to get host port for SSH container within 30 seconds.")

        print(f"SSH container with combined auth running on host port: {host_port}")

        # Wait for SSH server to be ready
        ready = False
        for i in range(60):  # wait up to 60 seconds
            try:

                async def check_ssh_combined():
                    # Check password authentication
                    try:
                        conn_pw = await asyncssh.connect(
                            docker_host_ip,
                            port=int(host_port),
                            username=ssh_user,
                            password=ssh_password,
                            known_hosts=None,
                        )
                        conn_pw.close()
                        print("Password auth check successful.")
                    except (asyncssh.Error, OSError) as e:
                        print(f"Password auth check failed: {e}")
                        return False

                    # Check public key authentication (unencrypted)
                    try:
                        conn_key = await asyncssh.connect(
                            docker_host_ip,
                            port=int(host_port),
                            username=ssh_user,
                            client_keys=[asyncssh.import_private_key(ssh_keys["unencrypted_private"])],
                            known_hosts=None,
                        )
                        conn_key.close()
                        print("Public key auth check successful.")
                    except (asyncssh.Error, OSError) as e:
                        print(f"Public key auth check failed: {e}")
                        return False

                    return True

                if await check_ssh_combined():
                    print(f"SSH server with combined auth is ready after {i+1} seconds.")
                    ready = True
                    break
            except Exception as e:
                print(f"Error during SSH readiness check: {e}")
                pass
            time.sleep(1)

        if not ready:
            print("\nSSH server with combined auth did not become ready in time. Container logs:")
            if container:
                print(container.logs().decode("utf-8"))
            raise RuntimeError("SSH server with combined auth did not become ready in time.")

        yield container, docker_host_ip, host_port, ssh_user, ssh_password, ssh_keys["unencrypted_private"]
    finally:
        if container:
            print("Stopping and removing ssh-test-container-combined-auth...")
            print("=========================== Container Logs Start ===========================")
            print(container.logs().decode("utf-8"))
            print("=========================== Container Logs End ===========================")
            container.stop()
            container.remove()


@pytest.fixture(scope="session")
def docker_host_ip():
    """Determines the Docker host IP for connecting to containers."""
    docker_host = os.environ.get("DOCKER_HOST")

    # Regex to match tcp://hostname:port, unix://socket, or ip:port
    regex = r"^(?:(tcp|unix)://)?([a-zA-Z0-9.-]+)(?::\d+)?$"

    if docker_host:
        match = re.match(regex, docker_host)
        if match:
            protocol, host = match.groups()
            if protocol == "unix":
                # For unix sockets, connections are typically local, but asyncssh needs an IP
                # In this case, 'localhost' is usually appropriate for host-to-container communication
                return "localhost"
            return host  # Return the IP or hostname
    return "localhost"  # Default for local Docker setup


@pytest.mark.asyncio
async def test_ssh_pipe_with_hello_world_emitter(ssh_container_combined_auth):
    """
    Tests SshPipe by connecting to an SSHD container that emits "hello world" every second.
    This test uses password authentication.
    """
    container, host, port, username, password, _ = ssh_container_combined_auth

    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} with password authentication...")
    pipe = None
    try:
        emitter_command = "sh -c 'while true; do echo \"hello world\"; sleep 1; done'"
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            known_hosts=None,
            timeout=10,
            command=emitter_command,
        )

        received_event = asyncio.Event()
        received_data = []

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_hello_world_emitter: Received data: {data!r}")
            received_data.append(data)
            # The data should be bytes, so compare directly with bytes literal
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
async def test_ssh_pipe_with_password_authentication(ssh_container_combined_auth):
    """
    Tests SshPipe using username and password authentication.
    This is a dedicated test for password authentication, ensuring it works as expected.
    """
    container, host, port, username, password, _ = ssh_container_combined_auth

    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} with password authentication...")
    pipe = None
    try:
        # Use a simple command to verify connection, e.g., 'echo'
        test_command = "echo 'Password auth successful!'"
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            known_hosts=None,
            timeout=10,
            command=test_command,
        )

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
        pytest.fail(f"SSH connection timed out with password authentication.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during password authentication test: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed for password authentication test.")


@pytest.mark.asyncio
async def test_ssh_pipe_with_plain_key_authentication(ssh_container_combined_auth, ssh_keys):
    """
    Tests SshPipe using a plain (unencrypted) SSH key for authentication.
    This test is designed to run when sshd_container is configured for plain key auth.
    """
    container, host, port, username, _, unencrypted_private_key_content = ssh_container_combined_auth

    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} with plain key authentication...")

    pipe = None
    try:
        test_command = "echo 'Plain key auth successful!'"
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            ssh_key_content=unencrypted_private_key_content,  # Use the private key content for authentication
            known_hosts=None,
            timeout=10,
            command=test_command,
        )

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
        pytest.fail(f"SSH connection timed out with plain key authentication.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during plain key authentication test: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed for plain key authentication test.")


@pytest.mark.asyncio
async def test_ssh_pipe_with_encrypted_key_authentication(ssh_container_combined_auth, ssh_keys):
    """
    Tests SshPipe using an encrypted SSH key with a passphrase for authentication.
    This test is designed to run when sshd_container is configured for encrypted key auth.
    """
    container, host, port, username, _, _ = ssh_container_combined_auth

    private_key_content = ssh_keys["encrypted_private"]
    passphrase = ssh_keys["passphrase"]

    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} with encrypted key authentication...")

    pipe = None
    try:
        test_command = "echo 'Encrypted key auth successful!'"
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            ssh_key_content=private_key_content,  # Use the private key content for authentication
            ssh_key_passphrase=passphrase,  # Pass the passphrase
            known_hosts=None,
            timeout=10,
            command=test_command,
        )

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
        pytest.fail(f"SSH connection timed out.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during encrypted key authentication test: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed for encrypted key authentication test.")


@pytest.mark.asyncio
async def test_ssh_pipe_with_echo_back_command(ssh_container_combined_auth):
    """
    Tests SshPipe by connecting to an SSHD container and running 'echo_back.py' to echo back input.
    This test uses password authentication.
    """
    container, host, port, username, password, _ = ssh_container_combined_auth

    print(
        f"\nAttempting SshPipe connection to {host}:{port} as user {username} for echo-back test (using echo_back.py) with password authentication..."
    )
    pipe = None
    try:
        # Use the custom Python echo-back script
        echo_back_command = "python3 /usr/local/bin/echo_back.py"
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            known_hosts=None,  # Disable host key checking for test container
            timeout=10,
            command=echo_back_command,
        )

        received_data_chunks = []
        received_event = asyncio.Event()

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_echo_back_command: Received data chunk: {data!r}")
            received_data_chunks.append(data)
            # For echo-back, we expect the full message to be returned
            # We'll set the event once we receive some data.
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
        await pipe.write_eof()  # Signal EOF to the remote process

    except asyncssh.Error as e:
        pytest.fail(f"SSH connection or command failed: {e}")
    except asyncio.TimeoutError:
        pytest.fail(f"SSH connection timed out.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed.")


@pytest.mark.asyncio
async def test_ssh_pipe_with_binary_data(ssh_container_combined_auth):
    """
    Tests SshPipe by connecting to an SSHD container that emits raw binary data.
    This test uses password authentication.
    """
    container, host, port, username, password, _ = ssh_container_combined_auth

    print(
        f"\nAttempting SshPipe connection to {host}:{port} as user {username} for binary data emitter test with password authentication..."
    )
    pipe = None
    try:
        # Python script to continuously emit binary data
        # Using os.write(1, ...) to write raw bytes to stdout
        emitter_binary_command = "python3 /usr/local/bin/binary_emitter.py"
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            known_hosts=None,  # Disable host key checking for test container
            timeout=10,
            command=emitter_binary_command,
        )

        received_event = asyncio.Event()
        received_data_chunks = []
        expected_binary_pattern = b"\xde\xad\xbe\xef\x00\x01\x02\x03\x80\xff\x7f"  # Updated pattern without newlines

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_binary_data: Received data chunk: {data!r}")
            # Strip any potential carriage returns or newlines added by the shell
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
        pytest.fail(f"SSH connection timed out.")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")
    finally:
        if pipe:
            await pipe.terminate()
            print("SshPipe closed.")
