import sys
import os
import pytest
import docker
import asyncssh
import re
import time
import asyncio

from cbor_rpc.ssh.ssh_pipe import SshServer
from tests.integration.rpc_test_helpers import get_stdio_server_script_path as _get_stdio_server_script_path

# Ensure the project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

TEST_SSH_USER = "testuser"
TEST_SSH_PASSWORD = "testpassword"
SSHD_IMAGE_NAME = "cbor-rpc-py-sshd-python"
SSHD_CONTAINER_NAME = "test-sshd-container"
# Assuming running from root
SSHD_DOCKERFILE_PATH = "./tests/docker/sshd-python"


@pytest.fixture
def get_stdio_server_script_path():
    return _get_stdio_server_script_path()

@pytest.fixture(scope="session")
def ssh_keys():
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
    client = docker.from_env()
    yield client
    client.close()

@pytest.fixture(scope="session")
def test_network(docker_client: docker.DockerClient):
    network_name = "test-ssh-network"
    try:
        network = docker_client.networks.get(network_name)
        network.remove()
    except docker.errors.NotFound:
        pass

    network = docker_client.networks.create(network_name, driver="bridge")
    yield network
    network.remove()

@pytest.fixture(scope="session")
def docker_host_ip():
    docker_host = os.environ.get("DOCKER_HOST")

    regex = r"^(?:(tcp|unix)://)?([a-zA-Z0-9.-]+)(?::\d+)?$"

    if docker_host:
        match = re.match(regex, docker_host)
        if match:
            protocol, host = match.groups()
            if protocol == "unix":
                return "localhost"
            return host
    return "localhost"

@pytest.fixture(scope="session")
def ssh_server_config(docker_client: docker.DockerClient, test_network, docker_host_ip, ssh_keys):
    container_name = "ssh-test-container-combined-auth"
    ssh_user = TEST_SSH_USER
    ssh_password = TEST_SSH_PASSWORD
    public_key = ssh_keys["unencrypted_public"]

    try:
        existing_container = docker_client.containers.get(container_name)
        # Check if it is running
        if existing_container.status == 'running':
             existing_container.stop()
        existing_container.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        print(f"Error cleaning up existing container: {e}")

    print(f"\nBuilding Docker image '{SSHD_IMAGE_NAME}' from '{SSHD_DOCKERFILE_PATH}'...")
    try:
        docker_client.images.build(
            tag=SSHD_IMAGE_NAME,
            path=".",
            dockerfile="tests/docker/sshd-python/Dockerfile",
            rm=True,
        )
    except docker.errors.BuildError as e:
        print(f"Failed to build Docker image: {e}")
        for line in e.build_log:
             print(line)
        raise RuntimeError(f"Failed to build Docker image '{SSHD_IMAGE_NAME}'.")

    print(f"\nStarting {SSHD_IMAGE_NAME} container...")
    container = docker_client.containers.run(
        SSHD_IMAGE_NAME,
        detach=True,
        ports={"2222/tcp": None},
        network=test_network.name,
        name=container_name,
        environment={
            "PUID": "1000",
            "PGID": "1000",
            "TZ": "Etc/UTC",
            "PASSWORD_ACCESS": "true",
            "USER_NAME": ssh_user,
            "USER_PASSWORD": ssh_password,
            "PUBLIC_KEY": public_key,
        },
        restart_policy={"Name": "no"},
    )

    try:
        container.reload()
        host_port = None
        ready = False
        
        # Wait for port mapping
        for _ in range(30):
            container.reload()
            if "2222/tcp" in container.ports and container.ports["2222/tcp"]:
                host_port = container.ports["2222/tcp"][0]["HostPort"]
                break
            time.sleep(1)
            
        if not host_port:
             raise RuntimeError("Port 2222 not exposed")

        # Wait for SSH to be ready
        async def check_ssh_combined():
            try:
                # Try password auth
                conn = await asyncssh.connect(
                    docker_host_ip,
                    port=int(host_port),
                    username=ssh_user,
                    password=ssh_password,
                    known_hosts=None,
                )
                conn.close()
                return True
            except (asyncssh.Error, OSError):
                return False
        
        for i in range(60):
            if asyncio.run(check_ssh_combined()):
                ready = True
                break
            time.sleep(1)

        if not ready:
            print(container.logs().decode('utf-8'))
            raise RuntimeError("SSH Server not ready")

        yield container, docker_host_ip, host_port, ssh_user, ssh_password, ssh_keys["unencrypted_private"]

    finally:
        print("Stopping SSH Server container...")
        print("================ Logs ================")
        try:
            print(container.logs().decode('utf-8'))
        except:
             pass
        print("======================================")
        container.stop()
        container.remove()

@pytest.fixture
async def ssh_server(ssh_server_config):
    """
    Fixture providing an SshServer connected to the server.
    """
    container, host, port, username, password, _ = ssh_server_config
    server = await SshServer.connect(
        host=host,
        port=port,
        username=username,
        password=password,
        known_hosts=None
    )
    yield server
    await server.close()


