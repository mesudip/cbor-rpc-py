#!/usr/bin/env python3
import sys
import os

# Read from stdin and write to stdout
# Ensure binary mode for consistent byte handling
stdin_fd = sys.stdin.buffer.fileno()
stdout_fd = sys.stdout.buffer.fileno()

while True:
    try:
        # Read a chunk of data from stdin
        data = os.read(stdin_fd, 4096)  # Read up to 4096 bytes
        if not data:
            # EOF reached on stdin
            break
        # Write the data to stdout
        os.write(stdout_fd, data)
        # Explicitly flush stdout to ensure data is sent immediately
        sys.stdout.buffer.flush()
    except BrokenPipeError:
        # stdout pipe was closed by the reader
        break
    except Exception as e:
        # Log any other errors to stderr
        sys.stderr.write(f"Error in echo_back.py: {e}\n".encode("utf-8"))
        sys.stderr.flush()
        break
