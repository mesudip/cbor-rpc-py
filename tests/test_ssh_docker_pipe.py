import asyncio
import pytest
import docker
import time
import asyncssh
import os
import re

from cbor_rpc.ssh.ssh_pipe import SshPipe

# Define a test user and password for the SSHD container
TEST_SSH_USER = "testuser"
TEST_SSH_PASSWORD = "testpassword"
SSHD_IMAGE_NAME = "cbor-rpc-py-sshd-python" # Custom image name
SSHD_CONTAINER_NAME = "test-sshd-container"
SSHD_DOCKERFILE_PATH = "./tests/docker/sshd-python" # Path to the Dockerfile

@pytest.fixture(scope="session")
def docker_client():
    """Provides a Docker client instance."""
    client = docker.from_env()
    yield client
    client.close()

@pytest.fixture(scope="session")
async def sshd_container(docker_client: docker.DockerClient, docker_host_ip):
    """
    Starts an SSHD Docker container, configures it, and yields its connection details.
    """
    container = None
    host_port = None

    # Ensure previous container is stopped and removed
    try:
        existing_container = docker_client.containers.get(SSHD_CONTAINER_NAME)
        print(f"Found existing container '{SSHD_CONTAINER_NAME}'. Stopping and removing...")
        existing_container.stop()
        existing_container.remove()
        print(f"Removed existing container '{SSHD_CONTAINER_NAME}'.")
    except docker.errors.NotFound:
        print(f"No existing container '{SSHD_CONTAINER_NAME}' found. Proceeding.")
    except Exception as e:
        print(f"Error cleaning up existing container: {e}")
        # Do not raise, try to proceed with run

    # Build the custom Docker image
    print(f"\nBuilding custom Docker image '{SSHD_IMAGE_NAME}' from {SSHD_DOCKERFILE_PATH}...")
    try:
        # Use path to Dockerfile directory as context
        image, build_logs = docker_client.images.build(
            path=SSHD_DOCKERFILE_PATH,
            tag=SSHD_IMAGE_NAME,
            rm=True # Remove intermediate containers
        )
        for chunk in build_logs:
            if 'stream' in chunk:
                print(chunk['stream'], end='')
        print(f"Successfully built custom image '{SSHD_IMAGE_NAME}'.")
    except docker.errors.BuildError as e:
        print(f"Error building Docker image: {e}")
        for line in e.build_log:
            if 'stream' in line:
                print(line['stream'], end='')
        raise
    except Exception as e:
        print(f"An unexpected error occurred during Docker image build: {e}")
        raise

    print(f"Starting {SSHD_CONTAINER_NAME} container...")
    try:
        container = docker_client.containers.run(
            SSHD_IMAGE_NAME, # Use the custom image
            detach=True,
            ports={'2222/tcp': None},  # Map container's SSH port (2222) to a random host port
            name=SSHD_CONTAINER_NAME,
            environment={
                "PUID": os.getuid(),
                "PGID": os.getgid(),
                "USER_PASSWORD": TEST_SSH_PASSWORD, # Correct variable for password
                "USER_NAME": TEST_SSH_USER, # Correct variable for username
                "PUBLIC_KEY": "", # No public key for password auth
                "TZ": "UTC",
                "SUDO_ACCESS": "true", # Allow sudo for testuser if needed
                "EXPOSE_SSH_PORT": "2222", # Explicitly expose port 2222
                "PASSWORD_ACCESS": "true" # Enable password authentication
            },
            restart_policy={"Name": "no"}
        )

        # Wait for port mapping to be available
        max_retries = 10
        for attempt in range(max_retries):
            container.reload()
            if '2222/tcp' in container.ports and container.ports['2222/tcp']:
                host_port = container.ports['2222/tcp'][0]['HostPort']
                print(f"Port 2222/tcp mapped to host port {host_port} after {attempt+1} attempts.")
                break
            print(f"Attempt {attempt+1}/{max_retries}: Port 2222/tcp not yet mapped. Retrying...")
            time.sleep(1)
        else:
            print(f"Error: Port 2222/tcp not mapped on host after {max_retries} attempts. Container ports: {container.ports}")
            print("Container logs:")
            print(container.logs().decode('utf-8'))
            raise RuntimeError("Failed to map SSHD port.")
            
        print(f"SSHD container running on {docker_host_ip}:{host_port}")
        print(f"SSHD container running on {docker_host_ip}:{host_port}")

        # Wait for SSHD to be ready
        ready = False
        for i in range(60):  # Wait up to 60 seconds
            try:
                # Try to connect using asyncssh to check if the server is up
                # Use a short timeout for the readiness check
                conn = await asyncio.wait_for(
                    asyncssh.connect(
                        docker_host_ip,
                        port=int(host_port),
                        options=asyncssh.SSHClientConnectionOptions(
                            username=TEST_SSH_USER,
                            password=TEST_SSH_PASSWORD,
                            known_hosts=None # Disable host key checking for test container
                        )
                    ),
                    timeout=5
                )
                conn.close() # Close the temporary connection used for readiness check
                await conn.wait_closed()
                print(f"SSHD is ready after {i+1} seconds.")
                ready = True
                break
            except (asyncssh.Error, asyncio.TimeoutError, ConnectionRefusedError) as e:
                # print(f"SSHD not ready yet: {e}") # Uncomment for verbose debugging
                pass
            time.sleep(1)

        if not ready:
            print("\nSSHD container did not become ready in time. Container logs:")
            if container:
                print(container.logs().decode('utf-8'))
            raise RuntimeError("SSHD container did not become ready in time.")

        # Python3 is now pre-installed in the custom Docker image, so no runtime installation needed.
        print("Python3 is pre-installed in the custom Docker image.")

        yield docker_host_ip, int(host_port), TEST_SSH_USER, TEST_SSH_PASSWORD

    finally:
        if container:
            print(f"Stopping and removing {SSHD_CONTAINER_NAME}...")
            container.stop()
            container.remove()
            print(f"Removed {SSHD_CONTAINER_NAME}.")

@pytest.fixture(scope="session")
def docker_host_ip():
    """Determines the Docker host IP for connecting to containers."""
    docker_host = os.environ.get("DOCKER_HOST")
    
    # Regex to match tcp://hostname:port, unix://socket, or ip:port
    regex = r'^(?:(tcp|unix)://)?([a-zA-Z0-9.-]+)(?::\d+)?$'
    
    if docker_host:
        match = re.match(regex, docker_host)
        if match:
            protocol, host = match.groups()
            if protocol == "unix":
                # For unix sockets, connections are typically local, but asyncssh needs an IP
                # In this case, 'localhost' is usually appropriate for host-to-container communication
                return "localhost" 
            return host  # Return the IP or hostname
    return "localhost" # Default for local Docker setup

@pytest.mark.asyncio
async def test_ssh_pipe_with_hello_world_emitter(sshd_container):
    """
    Tests SshPipe by connecting to an SSHD container that emits "hello world" every second.
    """
    host, port, username, password = sshd_container
    
    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username}...")
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
            command=emitter_command
        )

        received_event = asyncio.Event()
        received_data = []

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_hello_world_emitter: Received data: {data!r}")
            received_data.append(data)
            # The data should be bytes, so compare directly with bytes literal
            if b"hello world" in data:
                received_event.set()

        pipe.pipeline('data', on_data_callback)

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
async def test_ssh_pipe_with_echo_back_command(sshd_container):
    """
    Tests SshPipe by connecting to an SSHD container and running 'echo_back.py' to echo back input.
    """
    host, port, username, password = sshd_container
    
    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} for echo-back test (using echo_back.py)...")
    pipe = None
    try:
        # Use the custom Python echo-back script
        echo_back_command = "python3 /usr/local/bin/echo_back.py"
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            known_hosts=None, # Disable host key checking for test container
            timeout=10,
            command=echo_back_command
        )

        received_data_chunks = []
        received_event = asyncio.Event()

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_echo_back_command: Received data chunk: {data!r}")
            received_data_chunks.append(data)
            # For echo-back, we expect the full message to be returned
            # We'll set the event once we receive some data.
            received_event.set()

        pipe.pipeline('data', on_data_callback)

        test_message = b"This is a test message for echo back\n"
        print(f"Writing data to pipe: {test_message!r}")
        await pipe.write(test_message)
        await asyncio.sleep(1)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive any data within 10 seconds.")

        full_received_data = b"".join(received_data_chunks)
        print(f"Received data from pipe: {full_received_data!r}")

        # The Python echo_back.py script should echo back exactly what it receives.
        # Strip any potential carriage returns or newlines added by the shell/terminal.
        assert full_received_data.strip() == test_message.strip(), \
            f"Received data {full_received_data!r} should exactly match sent data {test_message!r}"
        print("Verification successful: Data echoed correctly by 'echo_back.py' script.")
        await pipe.write_eof() # Signal EOF to the remote process

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
async def test_ssh_pipe_with_binary_data(sshd_container):
    """
    Tests SshPipe by connecting to an SSHD container that emits raw binary data.
    """
    host, port, username, password = sshd_container
    
    print(f"\nAttempting SshPipe connection to {host}:{port} as user {username} for binary data emitter test...")
    pipe = None
    try:
        # Python script to continuously emit binary data
        # Using os.write(1, ...) to write raw bytes to stdout
        emitter_binary_command = (
            "python3 /usr/local/bin/binary_emitter.py"
        )
        pipe = await SshPipe.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            known_hosts=None, # Disable host key checking for test container
            timeout=10,
            command=emitter_binary_command
        )

        received_event = asyncio.Event()
        received_data_chunks = []
        expected_binary_pattern = b'\xDE\xAD\xBE\xEF\x00\x01\x02\x03\x80\xFF\x7F' # Updated pattern without newlines

        def on_data_callback(data):
            print(f"test_ssh_pipe_with_binary_data: Received data chunk: {data!r}")
            # Strip any potential carriage returns or newlines added by the shell
            cleaned_data = data.replace(b'\r', b'').replace(b'\n', b'')
            received_data_chunks.append(cleaned_data)
            if expected_binary_pattern in cleaned_data:
                received_event.set()

        pipe.pipeline('data', on_data_callback)

        try:
            await asyncio.wait_for(received_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("Did not receive expected binary pattern within 10 seconds.")

        full_received_data = b"".join(received_data_chunks)
        print(f"Received total {len(full_received_data)} bytes. First 50 bytes: {full_received_data[:50]!r}")
        
        assert expected_binary_pattern in full_received_data, \
            f"Expected binary pattern {expected_binary_pattern!r} not found in received data {full_received_data!r}"
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
