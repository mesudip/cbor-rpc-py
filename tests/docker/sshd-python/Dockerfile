# Use the linuxserver/openssh-server as base image
FROM linuxserver/openssh-server

# Install python3 using apk
RUN apk add --no-cache python3

# Copy the binary_emitter.py script into the container
COPY binary_emitter.py /usr/local/bin/binary_emitter.py
RUN chmod +x /usr/local/bin/binary_emitter.py

# Copy the echo_back.py script into the container
COPY echo_back.py /usr/local/bin/echo_back.py
RUN chmod +x /usr/local/bin/echo_back.py
