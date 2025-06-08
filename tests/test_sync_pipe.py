import pytest
from typing import Any, Tuple
from cbor_rpc.sync_pipe import SyncPipe

def test_create_pair():
    # Positive case: Creating a pair of sync pipes
    pipe1, pipe2 = SyncPipe.create_pair()
    assert isinstance(pipe1, SyncPipe)
    assert isinstance(pipe2, SyncPipe)

def test_write_read():
    # Positive case: Writing and reading a chunk successfully
    pipe1, pipe2 = SyncPipe.create_pair()

    assert pipe1.write("test_chunk") is True
    assert pipe2.read() == "test_chunk"

def test_close_pipe():
    # Positive case: Closing the pipe
    pipe1, pipe2 = SyncPipe.create_pair()
    pipe1.close()

    with pytest.raises(Exception):
        pipe1.read()

    assert pipe2.is_closed() is True

def test_write_after_close():
    # Negative case: Writing to a closed pipe
    pipe1, pipe2 = SyncPipe.create_pair()
    pipe1.close()

    assert pipe1.write("test_chunk") is False

def test_read_timeout():
    # Positive case: Reading with timeout
    pipe1, _ = SyncPipe.create_pair()

    assert pipe1.read(timeout=0.1) is None

def test_bidirectional_communication():
    # Positive case: Bidirectional communication between pipes
    pipe1, pipe2 = SyncPipe.create_pair()

    assert pipe1.write("test_chunk") is True
    assert pipe2.read() == "test_chunk"

    assert pipe2.write("response_chunk") is True
    assert pipe1.read() == "response_chunk"

def test_queue_size():
    # Positive case: Checking queue size
    pipe1, _ = SyncPipe.create_pair()

    pipe1.write("chunk1")
    pipe1.write("chunk2")

    assert pipe1.available() == 2

if __name__ == "__main__":
    pytest.main()
