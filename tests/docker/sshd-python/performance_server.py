import asyncio
import os
import sys
import logging
import random

# Ensure cbor_rpc is in path
# Try to find where cbor_rpc is.
current_dir = os.path.dirname(os.path.abspath(__file__))
# If running from /usr/local/bin, we might need to look elsewhere.
# But Dockerfile copied cbor_rpc to ./cbor_rpc relative to WORKDIR.
# Let's blindly add possible locations
sys.path.append("/app")
sys.path.append("/")
sys.path.append(os.getcwd())

from cbor_rpc import RpcV1Server
from cbor_rpc.rpc.context import RpcCallContext

# Setup logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class PerformanceServer(RpcV1Server):
    async def validate_event_broadcast(self, connection_id, topic, message):
        return True

    async def handle_method_call(self, connection_id, context, method, args):
        if method == "echo":
            # Just return whatever is sent
            return args[0]
        elif method == "log.info":
            context.logger.info(*args)
            await asyncio.sleep(0.1)  # Simulate some delay in logging

        elif method == "log.warn":
            context.logger.warn(*args)
            await asyncio.sleep(0.1)  # Simulate some delay in logging

        elif method == "log.error":
            context.logger.error(*args)
            await asyncio.sleep(0.1)  # Simulate some delay in logging

        elif method == "log.crit":
            context.logger.crit(*args)
            await asyncio.sleep(0.1)  # Simulate some delay in logging

        elif method == "sleep_with_progress":
            context.progress(0, "Starting")
            await asyncio.sleep(args[0] / 2)
            context.progress(50, "Halfway")
            await asyncio.sleep(args[0] / 2)
        elif method == "cancel_me_before":
            await asyncio.sleep(args[0])
        elif method == "download_random":
            size = args[0]
            # Generate random bytes? Generating 10MB random might be slow in python if clear.
            # faster to just multiply specific bytes
            return os.urandom(size)
        elif method == "upload_random":
            data = args[0]
            return len(data)
        elif method == "echo":
            # Just return whatever is sent
            return args[0]
        elif method == "add":
            return sum(args)
        elif method == "multiply":
            result = 1
            for arg in args:
                result *= arg
            return result
        elif method == "random_delay":
            delay = args[0]
            await asyncio.sleep(delay)
            return f"Delayed for {delay} seconds"
        elif method == "sleep":
            delay = args[0]
            await asyncio.sleep(delay)
            return f"Slept for {delay} seconds"
        else:
            raise ValueError(f"Unknown method {method}")
