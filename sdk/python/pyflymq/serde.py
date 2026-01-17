import json
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Union

class Serializer(ABC):
    @abstractmethod
    def serialize(self, topic: str, data: Any) -> bytes:
        pass

class Deserializer(ABC):
    @abstractmethod
    def deserialize(self, topic: str, data: bytes) -> Any:
        pass

class StringSerializer(Serializer):
    def serialize(self, topic: str, data: Any) -> bytes:
        if data is None:
            return None
        if isinstance(data, str):
            return data.encode('utf-8')
        return str(data).encode('utf-8')

class StringDeserializer(Deserializer):
    def deserialize(self, topic: str, data: bytes) -> str:
        if data is None:
            return None
        return data.decode('utf-8')

class BytesSerializer(Serializer):
    def serialize(self, topic: str, data: Any) -> bytes:
        if data is None:
            return None
        if isinstance(data, bytes):
            return data
        raise ValueError(f"BytesSerializer expects bytes, got {type(data)}")

class BytesDeserializer(Deserializer):
    def deserialize(self, topic: str, data: bytes) -> bytes:
        return data

class JsonSerializer(Serializer):
    def __init__(self, encoder: Optional[Callable] = None):
        self._encoder = encoder

    def serialize(self, topic: str, data: Any) -> bytes:
        if data is None:
            return None
        return json.dumps(data, default=self._encoder).encode('utf-8')

class JsonDeserializer(Deserializer):
    def __init__(self, object_hook: Optional[Callable] = None):
        self._object_hook = object_hook

    def deserialize(self, topic: str, data: bytes) -> Any:
        if data is None:
            return None
        return json.loads(data.decode('utf-8'), object_hook=self._object_hook)

class Serdes:
    @staticmethod
    def string():
        return StringSerializer(), StringDeserializer()
    
    @staticmethod
    def bytes():
        return BytesSerializer(), BytesDeserializer()
    
    @staticmethod
    def json(encoder=None, object_hook=None):
        return JsonSerializer(encoder), JsonDeserializer(object_hook)
