import cbor2
from io import BytesIO
from typing import Any, Union

from .base import Transformer, AsyncTransformer
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException

# Use the pure-Python CBORDecoder to avoid buffering issues with the C extension
# when using stream-slicing logic.
try:
    from cbor2._decoder import CBORDecoder as PythonCBORDecoder
except ImportError:
    from cbor2 import CBORDecoder as PythonCBORDecoder

# Import pure-Python break_marker / CBORDecodeError so we can handle both
# C-extension and pure-Python backends uniformly.
try:
    from cbor2._types import break_marker as _py_break_marker
except ImportError:
    _py_break_marker = cbor2.break_marker



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

    def __init__(self, max_buffer_bytes: int = 1024 * 1024*50):
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
        stream = BytesIO(self._buffer)
        decoder = PythonCBORDecoder(stream)
        try:
            obj = decoder.decode()
        except Exception as e:
            # Normalize value errors to CBORDecodeError for consistent API behavior.
            if isinstance(e, cbor2.CBORDecodeValueError) or type(e).__name__ == "CBORDecodeValueError":
                raise cbor2.CBORDecodeError(str(e)) from e
            raise

        if obj is cbor2.break_marker or obj is _py_break_marker:
            raise cbor2.CBORDecodeError("Unexpected break marker")

        self._buffer = self._buffer[stream.tell():]
        return obj
