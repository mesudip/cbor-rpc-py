import json
from io import BytesIO
from typing import Any, Union

import ijson
from ijson.common import IncompleteJSONError

from .base import AsyncTransformer, Transformer
from cbor_rpc.transformer.base.base_exception import NeedsMoreDataException


class JsonTransformer(Transformer[Any, Any]):
    """
    A transformer that encodes Python objects to JSON strings and decodes JSON strings back to Python objects.
    """

    def __init__(self, encoding: str = "utf-8"):
        super().__init__()
        self.encoding = encoding

    def encode(self, data: Any) -> bytes:
        json_str = json.dumps(data, ensure_ascii=False)  # Always allow non-ASCII characters to pass through json.dumps
        return json_str.encode(self.encoding)

    def decode(self, data: Union[bytes, str, None]) -> Any:
        if data is None:
            raise TypeError("Expected bytes or str, got None")

        if isinstance(data, bytes):
            json_str = data.decode(self.encoding)
        elif isinstance(data, str):
            json_str = data
        else:
            raise TypeError(f"Expected bytes or str, got {type(data)}")

        return json.loads(json_str)


class JsonStreamTransformer(AsyncTransformer[Any, Any]):
    """Stream transformer that decodes concatenated JSON values from a byte stream."""

    def __init__(self, encoding: str = "utf-8", max_buffer_bytes: int = 1024 * 1024 * 50):
        super().__init__()
        self.encoding = encoding
        self._buffer = bytearray()
        self._max_buffer_bytes = max_buffer_bytes

    async def encode(self, data: Any) -> bytes:
        json_str = json.dumps(data, ensure_ascii=False)
        # Add a newline so multiple messages are unambiguous in the stream.
        return (json_str + "\n").encode(self.encoding)

    async def decode(self, data: Union[bytes, None]) -> Any:
        if data is not None:
            if not isinstance(data, bytes):
                raise TypeError(f"Expected bytes or None, got {type(data)}")
            self._buffer.extend(data)

        if len(self._buffer) > self._max_buffer_bytes:
            self._buffer.clear()
            raise OverflowError("JSON stream buffer exceeded max size")

        if not self._buffer:
            raise NeedsMoreDataException()

        try:
            obj, consumed = self._decode_one()
        except IncompleteJSONError:
            raise NeedsMoreDataException()

        if consumed <= 0:
            raise NeedsMoreDataException()

        self._buffer = self._buffer[consumed:]
        return obj

    def _decode_one(self) -> tuple[Any, int]:
        stream = BytesIO(self._buffer)
        try:
            iterator = ijson.items(stream, "")
            obj = next(iterator)
        except StopIteration:
            raise NeedsMoreDataException()
        consumed = stream.tell()
        return obj, consumed
