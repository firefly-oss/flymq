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

class AvroSerializer(Serializer):
    def __init__(self, schema: Optional[str] = None, encoder: Optional[Callable] = None):
        self._schema = schema
        self._encoder = encoder
        self._parsed_schema = None
        if schema:
            from fastavro import parse_schema
            import json
            self._parsed_schema = parse_schema(json.loads(schema))

    def serialize(self, topic: str, data: Any) -> bytes:
        if self._encoder:
            return self._encoder(topic, data)
        if not self._parsed_schema:
            raise ValueError("Avro serialization requires a schema")
        
        import io
        from fastavro import schemaless_writer
        rb = io.BytesIO()
        schemaless_writer(rb, self._parsed_schema, data)
        return rb.getvalue()

class AvroDeserializer(Deserializer):
    def __init__(self, schema: Optional[str] = None, decoder: Optional[Callable] = None):
        self._schema = schema
        self._decoder = decoder
        self._parsed_schema = None
        if schema:
            from fastavro import parse_schema
            import json
            self._parsed_schema = parse_schema(json.loads(schema))

    def deserialize(self, topic: str, data: bytes) -> Any:
        if self._decoder:
            return self._decoder(topic, data)
        if not self._parsed_schema:
            raise ValueError("Avro deserialization requires a schema")
        
        import io
        from fastavro import schemaless_reader
        rb = io.BytesIO(data)
        return schemaless_reader(rb, self._parsed_schema)

class ProtoSerializer(Serializer):
    def __init__(self, msg_class: Any = None, encoder: Optional[Callable] = None):
        self._msg_class = msg_class
        self._encoder = encoder

    def serialize(self, topic: str, data: Any) -> bytes:
        if self._encoder:
            return self._encoder(topic, data)
        
        if self._msg_class:
            if isinstance(data, self._msg_class):
                return data.SerializeToString()
            # If data is a dict, try to parse it into the msg_class
            from google.protobuf.json_format import ParseDict
            msg = self._msg_class()
            ParseDict(data, msg)
            return msg.SerializeToString()
        
        raise ValueError("Protobuf serialization requires a message class")

class ProtoDeserializer(Deserializer):
    def __init__(self, msg_class: Any = None, decoder: Optional[Callable] = None):
        self._msg_class = msg_class
        self._decoder = decoder

    def deserialize(self, topic: str, data: bytes) -> Any:
        if self._decoder:
            return self._decoder(topic, data)
        
        if not self._msg_class:
            raise ValueError("Protobuf deserialization requires a message class")
        
        msg = self._msg_class()
        msg.ParseFromString(data)
        return msg

class SerdeRegistry:
    _serializers = {}
    _deserializers = {}

    @classmethod
    def register(cls, name: str, serializer: Serializer, deserializer: Deserializer):
        cls._serializers[name] = serializer
        cls._deserializers[name] = deserializer

    @classmethod
    def get(cls, name: str) -> tuple[Optional[Serializer], Optional[Deserializer]]:
        return cls._serializers.get(name), cls._deserializers.get(name)

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

    @staticmethod
    def avro(schema=None, encoder=None, decoder=None):
        return AvroSerializer(schema, encoder), AvroDeserializer(schema, decoder)

    @staticmethod
    def protobuf(descriptor=None, encoder=None, decoder=None):
        return ProtoSerializer(descriptor, encoder), ProtoDeserializer(descriptor, decoder)

# Register default SerDes
s_str, d_str = Serdes.string()
SerdeRegistry.register("string", s_str, d_str)
s_bytes, d_bytes = Serdes.bytes()
SerdeRegistry.register("binary", s_bytes, d_bytes)
s_json, d_json = Serdes.json()
SerdeRegistry.register("json", s_json, d_json)
s_avro, d_avro = Serdes.avro()
SerdeRegistry.register("avro", s_avro, d_avro)
s_proto, d_proto = Serdes.protobuf()
SerdeRegistry.register("protobuf", s_proto, d_proto)
