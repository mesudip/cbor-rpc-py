import json
from typing import Any, Union
from .pipe import Transformer, Pipe


class JsonTransformer(Transformer[Any, Any]):
    """
    A transformer that encodes Python objects to JSON strings and decodes JSON strings back to Python objects.
    """
    
    def __init__(self, underlying_pipe: Pipe[Any, Any], encoding: str = 'utf-8'):
        """
        Initialize the JSON transformer.
        
        Args:
            underlying_pipe: The underlying pipe to transform
            encoding: Text encoding to use (default: 'utf-8')
        """
        super().__init__(underlying_pipe)
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
        try:
            json_str = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            return json_str.encode(self.encoding)
        except (TypeError, ValueError) as e:
            raise TypeError(f"Object is not JSON serializable: {e}")
        except UnicodeEncodeError as e:
            raise UnicodeEncodeError(f"Failed to encode JSON to {self.encoding}: {e}")
    
    async def decode(self, data: Union[bytes, str]) -> Any:
        """
        Decode JSON bytes/string to Python object.
        
        Args:
            data: JSON bytes or string to decode
            
        Returns:
            Decoded Python object
            
        Raises:
            json.JSONDecodeError: If data is not valid JSON
            UnicodeDecodeError: If bytes cannot be decoded
        """
        try:
            if isinstance(data, bytes):
                json_str = data.decode(self.encoding)
            elif isinstance(data, str):
                json_str = data
            else:
                raise TypeError(f"Expected bytes or str, got {type(data)}")
            
            return json.loads(json_str)
        except UnicodeDecodeError as e:
            raise UnicodeDecodeError(f"Failed to decode bytes as {self.encoding}: {e}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON: {e}")
    
    @classmethod
    def create_pair(cls, encoding: str = 'utf-8') -> tuple['JsonTransformer', 'JsonTransformer']:
        """
        Create a pair of connected JSON transformers.
        
        Args:
            encoding: Text encoding to use
            
        Returns:
            A tuple of (transformer1, transformer2)
        """
        pipe1, pipe2 = Pipe.create_pair()
        transformer1 = cls(pipe1, encoding)
        transformer2 = cls(pipe2, encoding)
        return transformer1, transformer2
