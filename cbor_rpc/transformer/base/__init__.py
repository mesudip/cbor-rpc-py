from typing import overload, Union
from cbor_rpc.pipe.event_pipe import EventPipe
from cbor_rpc.pipe.pipe import Pipe
from cbor_rpc.transformer.base.async_transformer import EventTransformerPipe
from cbor_rpc.transformer.base.sync_transformer import TransformerPipe
from cbor_rpc.transformer.base.transformer_base import Transformer


@overload
def applyTransformer(pipe: Pipe, transformer: Transformer) -> TransformerPipe: ...
@overload
def applyTransformer(pipe: EventPipe, transformer: Transformer) -> EventTransformerPipe: ...

def applyTransformer(pipe: Union[Pipe, EventPipe], transformer: Transformer) -> Union[TransformerPipe, EventTransformerPipe]:
    if isinstance(pipe, EventPipe):
        return EventTransformerPipe(pipe, transformer)
    elif isinstance(pipe, Pipe):
        return TransformerPipe(pipe, transformer)
    else:
        raise TypeError("Invalid pipe type")