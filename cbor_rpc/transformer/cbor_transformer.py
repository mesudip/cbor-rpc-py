import cbor2
from io import BytesIO
from typing import Any, Union

from .base import Transformer, AsyncTransformer
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException

# cbor2 5.x's public C decoder reads ahead, which makes stream.tell() unsuitable
# for slicing one item from a sequence of concatenated CBOR objects. Its private
# Python decoder does not read ahead. cbor2 6.x removed the Python implementation,
# and its public decoder reports the consumed stream position correctly.
try:
    from cbor2._decoder import CBORDecoder as StreamCBORDecoder
except ImportError:
    from cbor2 import CBORDecoder as StreamCBORDecoder


_CBOR_BREAK_BYTE = 0xFF
_CBOR_DECODE_VALUE_ERROR = getattr(cbor2, "CBORDecodeValueError", None)


def _is_eof_error(exc: Exception) -> bool:
    """Return True if *exc* signals incomplete CBOR data (C or Python backend)."""
    return isinstance(exc, (cbor2.CBORDecodeEOF, IndexError)) or type(exc).__name__ == "CBORDecodeEOF"


class CborTransformer(Transformer[Any, bytes]):
    """Encodes Python objects to CBOR bytes and decodes CBOR bytes back."""

    def encode(self, data: Any) -> bytes:
        return cbor2.dumps(data)

    def decode(self, data: Union[bytes, None]) -> Any:
        if data is None:
            raise TypeError("Expected bytes, got None")
        if not isinstance(data, bytes):
            raise TypeError(f"Expected bytes, got {type(data)}")
        try:
            return cbor2.loads(data)
        except cbor2.CBORDecodeEOF as e:
            raise cbor2.CBORDecodeError("Incomplete CBOR data for non-stream transformer") from e


class CborStreamTransformer(AsyncTransformer[Any, Any]):
    """Async stream transformer that decodes concatenated CBOR objects."""

    def __init__(self, max_buffer_bytes: int = 1024 * 1024 * 50):
        super().__init__()
        self._buffer = bytearray()
        self._max_buffer_bytes = max_buffer_bytes

    async def encode(self, data: Any) -> bytes:
        return cbor2.dumps(data)

    async def decode(self, data: Union[bytes, None]) -> Any:
        if data is not None:
            if not isinstance(data, bytes):
                raise TypeError(f"Expected bytes or None, got {type(data)}")
            self._buffer.extend(data)

        if len(self._buffer) > self._max_buffer_bytes:
            self._buffer.clear()
            raise OverflowError("CBOR stream buffer exceeded max size")

        if not self._buffer:
            raise NeedsMoreDataException()

        try:
            return self._decode_one()
        except Exception as e:
            if _is_eof_error(e):
                raise NeedsMoreDataException()
            raise

    # -- private helpers --------------------------------------------------

    def _decode_one(self) -> Any:
        """Decode exactly one CBOR object from the front of the buffer."""
        # A break byte is valid only inside an indefinite-length CBOR item. At
        # the top level, older cbor2 versions return a private sentinel while
        # newer versions return an opaque object. Reject it before decoding so
        # the behavior is consistent without depending on private sentinels.
        if self._buffer[0] == _CBOR_BREAK_BYTE:
            raise cbor2.CBORDecodeError("Unexpected break marker")

        stream = BytesIO(self._buffer)
        decoder = StreamCBORDecoder(stream)
        try:
            obj = decoder.decode()
        except Exception as e:
            # Normalize value errors to CBORDecodeError for consistent API behavior.
            if (_CBOR_DECODE_VALUE_ERROR is not None and isinstance(e, _CBOR_DECODE_VALUE_ERROR)) or type(
                e
            ).__name__ == "CBORDecodeValueError":
                raise cbor2.CBORDecodeError(str(e)) from e
            raise

        self._buffer = self._buffer[stream.tell() :]
        return obj
