# Use the linuxserver/openssh-server as base image
FROM linuxserver/openssh-server

# Install python3 using apk
RUN apk add --no-cache python3 py3-pip build-base python3-dev libffi-dev

# Install python dependencies globally
RUN pip install cbor2 ijson asyncssh --break-system-packages

# Enable Unix socket forwarding for tests
RUN echo "AllowStreamLocalForwarding yes" >> /etc/ssh/sshd_config

WORKDIR /app
ENV PYTHONPATH=/app

# Copy the binary_emitter.py script into the container
COPY tests/docker/sshd-python/binary_emitter.py /usr/local/bin/binary_emitter.py
RUN chmod +x /usr/local/bin/binary_emitter.py

# Copy the echo_back.py script into the container
COPY tests/docker/sshd-python/echo_back.py /usr/local/bin/echo_back.py
RUN chmod +x /usr/local/bin/echo_back.py

COPY tests/docker/sshd-python/rpc_server.py /usr/local/bin/rpc_server.py
RUN chmod +x /usr/local/bin/rpc_server.py

COPY tests/docker/sshd-python/performance_server.py /usr/local/bin/performance_server.py
RUN chmod +x /usr/local/bin/performance_server.py

COPY tests/docker/sshd-python/unix_socket_bridge.py /usr/local/bin/unix_socket_bridge.py
RUN chmod +x /usr/local/bin/unix_socket_bridge.py

COPY tests/docker/sshd-python/unix_socket_rpc_wrapper.py /usr/local/bin/unix_socket_rpc_wrapper.py
RUN chmod +x /usr/local/bin/unix_socket_rpc_wrapper.py

# Copy the library code
COPY cbor_rpc /app/cbor_rpc

# Copy the server script
COPY tests/docker/sshd-python/rpc_server.py /app/rpc_server.py
COPY tests/docker/sshd-python/performance_server.py /app/performance_server.py
COPY tests/docker/sshd-python/unix_socket_bridge.py /app/unix_socket_bridge.py
COPY tests/docker/sshd-python/unix_socket_rpc_wrapper.py /app/unix_socket_rpc_wrapper.py
