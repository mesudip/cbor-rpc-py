import cbor2
from io import BytesIO
from typing import Any, Union
from .base import Transformer, AsyncTransformer
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException


class CborTransformer(Transformer[Any, Any]):
    """
    A transformer that encodes Python objects to CBOR bytes and decodes CBOR bytes back to Python objects.
    """

    def __init__(self):
        super().__init__()

    def encode(self, data: Any) -> bytes:
        try:
            return cbor2.dumps(data)
        except Exception:
            raise

    def decode(self, data: Union[bytes, None]) -> Any:
        if data is None:
            raise TypeError("Expected bytes, got None")

        if not isinstance(data, bytes):
            raise TypeError(f"Expected bytes, got {type(data)}")

        try:
            # cbor2.loads can decode a single CBOR object from bytes
            return cbor2.loads(data)
        except cbor2.CBORDecodeEOF as e:
            # For a non-stream transformer, incomplete data is a decoding error
            raise cbor2.CBORDecodeError("Incomplete CBOR data for non-stream transformer") from e
        except cbor2.CBORDecodeError:
            # Re-raise other decoding errors
            raise


class CborStreamTransformer(AsyncTransformer[Any, Any]):
    """
    An async transformer that decodes a stream of concatenated CBOR objects.
    This is similar to how a JSON stream decoder would work, reading one object at a time.
    """

    def __init__(self):
        super().__init__()
        self._buffer = bytearray()

    async def encode(self, data: Any) -> bytes:
        try:
            return cbor2.dumps(data)
        except Exception:
            raise

    async def decode(self, data: Union[bytes, None]) -> Any:
        if data is not None:
            if not isinstance(data, bytes):
                raise TypeError(f"Expected bytes or None, got {type(data)}")
            self._buffer.extend(data)

        if not self._buffer:
            raise NeedsMoreDataException()

        try:
            stream = BytesIO(self._buffer)
            decoder = cbor2.CBORDecoder(stream)
            decoded_data = decoder.decode()

            bytes_consumed = stream.tell()
            self._buffer = self._buffer[bytes_consumed:]

            return decoded_data
        except cbor2.CBORDecodeEOF:
            raise NeedsMoreDataException()
        except cbor2.CBORDecodeError as e:
            original_exception = e
            # Discard bytes from the buffer until a valid CBOR object can be decoded or the buffer is exhausted.
            # This loop attempts to find the start of the next valid CBOR object.
            while self._buffer:
                # Discard one byte and try again
                self._buffer = self._buffer[1:]
                if not self._buffer:
                    break  # Buffer is empty, cannot recover further

                try:
                    stream = BytesIO(self._buffer)
                    decoder = cbor2.CBORDecoder(stream)
                    decoded_data = decoder.decode()

                    bytes_consumed = stream.tell()
                    self._buffer = self._buffer[bytes_consumed:]

                    return decoded_data  # Successfully decoded a new object
                except cbor2.CBORDecodeEOF:
                    # If we need more data after discarding some, it means we might be in the middle of a valid object
                    raise NeedsMoreDataException()
                except cbor2.CBORDecodeError:
                    # Still an error after discarding one byte, continue the loop to discard another
                    continue
            # If we reach here, the buffer is exhausted or we couldn't recover.
            # Re-raise the original exception as no valid CBOR object could be found.
            raise original_exception
        except Exception as e:
            # For other unexpected errors, clear the buffer and re-raise
            self._buffer = bytearray()
            raise e
