import json
from typing import Any, Union
from ..event.emitter import AbstractEmitter
from . import Transformer
from ..pipe.event_pipe import  EventPipe

class JsonTransformer(AbstractEmitter, Transformer[Any, Any]):
    """
    A transformer that encodes Python objects to JSON strings and decodes JSON strings back to Python objects.
    """

    def __init__(self, underlying_pipe: EventPipe[Any, Any], encoding: str = 'utf-8'):
        """
        Initialize the JSON transformer.

        Args:
            underlying_pipe: The underlying pipe to transform
            encoding: Text encoding to use (default: 'utf-8')
        """
        AbstractEmitter.__init__(self)
        Transformer.__init__(self, underlying_pipe)
        self.encoding = encoding

    async def encode(self, data: Any) -> bytes:
        """
        Encode Python object to JSON bytes.

        Args:
            data: Python object to encode

        Returns:
            JSON-encoded bytes

        Raises:
            TypeError: If data is not JSON serializable
            UnicodeEncodeError: If encoding fails
        """
        json_str = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        return json_str.encode(self.encoding)

    async def decode(self, data: Union[bytes, str, None]) -> Any:
        """
        Decode JSON bytes/string to Python object.

        Args:
            data: JSON bytes or string to decode

        Returns:
            Decoded Python object

        Raises:
            json.JSONDecodeError: If data is not valid JSON
            UnicodeDecodeError: If bytes cannot be decoded
            TypeError: If data is None or of invalid type
        """
        if data is None:
            raise TypeError("Expected bytes or str, got None")

        if isinstance(data, bytes):
            json_str = data.decode(self.encoding)
        elif isinstance(data, str):
            json_str = data
        else:
            raise TypeError(f"Expected bytes or str, got {type(data)}")

        return json.loads(json_str)

    @classmethod
    def create_pair(cls, encoding: str = 'utf-8') -> tuple['JsonTransformer', 'JsonTransformer']:
        """
        Create a pair of connected JSON transformers.

        Args:
            encoding: Text encoding to use

        Returns:
            A tuple of (transformer1, transformer2)
        """
        pipe1, pipe2 = EventPipe.create_pair()
        transformer1 = cls(pipe1, encoding)
        transformer2 = cls(pipe2, encoding)
        return transformer1, transformer2
