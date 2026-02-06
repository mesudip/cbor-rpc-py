#!/usr/bin/env python3
import os
import time
import binascii

hex_data = "DEADBEEF0001020380FF7F"  # Removed \x0A\x0D (LF and CR)
test_data = binascii.unhexlify(hex_data)

while True:
    os.write(1, test_data)
    time.sleep(0.1)
