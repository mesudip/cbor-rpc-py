import json
from typing import Any, Union
from .base import Transformer

class JsonTransformer(Transformer[Any, Any]):
    """
    A transformer that encodes Python objects to JSON strings and decodes JSON strings back to Python objects.
    """

    def __init__(self, encoding: str = 'utf-8'):
        super().__init__()
        self.encoding = encoding

    def encode(self, data: Any) -> bytes:
        try:
            json_str = json.dumps(data, ensure_ascii=False) # Always allow non-ASCII characters to pass through json.dumps
            return json_str.encode(self.encoding)
        except Exception as e:
            # Removed print statement as it was for debugging
            raise # Re-raise to be caught by EventTransformerPipe

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
