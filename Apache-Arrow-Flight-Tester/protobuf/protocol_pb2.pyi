from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ManagerMetadata(_message.Message):
    __slots__ = ("key", "s3_configuration", "azure_configuration")
    class S3Configuration(_message.Message):
        __slots__ = ("endpoint", "bucket_name", "access_key_id", "secret_access_key")
        ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
        ACCESS_KEY_ID_FIELD_NUMBER: _ClassVar[int]
        SECRET_ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
        endpoint: str
        bucket_name: str
        access_key_id: str
        secret_access_key: str
        def __init__(self, endpoint: _Optional[str] = ..., bucket_name: _Optional[str] = ..., access_key_id: _Optional[str] = ..., secret_access_key: _Optional[str] = ...) -> None: ...
    class AzureConfiguration(_message.Message):
        __slots__ = ("account_name", "access_key", "container_name")
        ACCOUNT_NAME_FIELD_NUMBER: _ClassVar[int]
        ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
        CONTAINER_NAME_FIELD_NUMBER: _ClassVar[int]
        account_name: str
        access_key: str
        container_name: str
        def __init__(self, account_name: _Optional[str] = ..., access_key: _Optional[str] = ..., container_name: _Optional[str] = ...) -> None: ...
    KEY_FIELD_NUMBER: _ClassVar[int]
    S3_CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    AZURE_CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    key: str
    s3_configuration: ManagerMetadata.S3Configuration
    azure_configuration: ManagerMetadata.AzureConfiguration
    def __init__(self, key: _Optional[str] = ..., s3_configuration: _Optional[_Union[ManagerMetadata.S3Configuration, _Mapping]] = ..., azure_configuration: _Optional[_Union[ManagerMetadata.AzureConfiguration, _Mapping]] = ...) -> None: ...

class NodeMetadata(_message.Message):
    __slots__ = ("url", "server_mode")
    class ServerMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CLOUD: _ClassVar[NodeMetadata.ServerMode]
        EDGE: _ClassVar[NodeMetadata.ServerMode]
    CLOUD: NodeMetadata.ServerMode
    EDGE: NodeMetadata.ServerMode
    URL_FIELD_NUMBER: _ClassVar[int]
    SERVER_MODE_FIELD_NUMBER: _ClassVar[int]
    url: str
    server_mode: NodeMetadata.ServerMode
    def __init__(self, url: _Optional[str] = ..., server_mode: _Optional[_Union[NodeMetadata.ServerMode, str]] = ...) -> None: ...

class TableMetadata(_message.Message):
    __slots__ = ("normal_tables", "time_series_tables")
    class NormalTableMetadata(_message.Message):
        __slots__ = ("name", "schema")
        NAME_FIELD_NUMBER: _ClassVar[int]
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        name: str
        schema: bytes
        def __init__(self, name: _Optional[str] = ..., schema: _Optional[bytes] = ...) -> None: ...
    class TimeSeriesTableMetadata(_message.Message):
        __slots__ = ("name", "schema", "error_bounds", "generated_column_expressions")
        class ErrorBound(_message.Message):
            __slots__ = ("type", "value")
            class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
                __slots__ = ()
                ABSOLUTE: _ClassVar[TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type]
                RELATIVE: _ClassVar[TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type]
            ABSOLUTE: TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type
            RELATIVE: TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type
            TYPE_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            type: TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type
            value: float
            def __init__(self, type: _Optional[_Union[TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type, str]] = ..., value: _Optional[float] = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        ERROR_BOUNDS_FIELD_NUMBER: _ClassVar[int]
        GENERATED_COLUMN_EXPRESSIONS_FIELD_NUMBER: _ClassVar[int]
        name: str
        schema: bytes
        error_bounds: _containers.RepeatedCompositeFieldContainer[TableMetadata.TimeSeriesTableMetadata.ErrorBound]
        generated_column_expressions: _containers.RepeatedScalarFieldContainer[bytes]
        def __init__(self, name: _Optional[str] = ..., schema: _Optional[bytes] = ..., error_bounds: _Optional[_Iterable[_Union[TableMetadata.TimeSeriesTableMetadata.ErrorBound, _Mapping]]] = ..., generated_column_expressions: _Optional[_Iterable[bytes]] = ...) -> None: ...
    NORMAL_TABLES_FIELD_NUMBER: _ClassVar[int]
    TIME_SERIES_TABLES_FIELD_NUMBER: _ClassVar[int]
    normal_tables: _containers.RepeatedCompositeFieldContainer[TableMetadata.NormalTableMetadata]
    time_series_tables: _containers.RepeatedCompositeFieldContainer[TableMetadata.TimeSeriesTableMetadata]
    def __init__(self, normal_tables: _Optional[_Iterable[_Union[TableMetadata.NormalTableMetadata, _Mapping]]] = ..., time_series_tables: _Optional[_Iterable[_Union[TableMetadata.TimeSeriesTableMetadata, _Mapping]]] = ...) -> None: ...

class Configuration(_message.Message):
    __slots__ = ("multivariate_reserved_memory_in_bytes", "uncompressed_reserved_memory_in_bytes", "compressed_reserved_memory_in_bytes", "transfer_batch_size_in_bytes", "transfer_time_in_seconds", "ingestion_threads", "compression_threads", "writer_threads")
    MULTIVARIATE_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    COMPRESSED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_BATCH_SIZE_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_TIME_IN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    INGESTION_THREADS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_THREADS_FIELD_NUMBER: _ClassVar[int]
    WRITER_THREADS_FIELD_NUMBER: _ClassVar[int]
    multivariate_reserved_memory_in_bytes: int
    uncompressed_reserved_memory_in_bytes: int
    compressed_reserved_memory_in_bytes: int
    transfer_batch_size_in_bytes: int
    transfer_time_in_seconds: int
    ingestion_threads: int
    compression_threads: int
    writer_threads: int
    def __init__(self, multivariate_reserved_memory_in_bytes: _Optional[int] = ..., uncompressed_reserved_memory_in_bytes: _Optional[int] = ..., compressed_reserved_memory_in_bytes: _Optional[int] = ..., transfer_batch_size_in_bytes: _Optional[int] = ..., transfer_time_in_seconds: _Optional[int] = ..., ingestion_threads: _Optional[int] = ..., compression_threads: _Optional[int] = ..., writer_threads: _Optional[int] = ...) -> None: ...

class UpdateConfiguration(_message.Message):
    __slots__ = ("setting", "new_value")
    class Setting(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        MULTIVARIATE_RESERVED_MEMORY_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        COMPRESSED_RESERVED_MEMORY_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        TRANSFER_BATCH_SIZE_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        TRANSFER_TIME_IN_SECONDS: _ClassVar[UpdateConfiguration.Setting]
    MULTIVARIATE_RESERVED_MEMORY_IN_BYTES: UpdateConfiguration.Setting
    UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: UpdateConfiguration.Setting
    COMPRESSED_RESERVED_MEMORY_IN_BYTES: UpdateConfiguration.Setting
    TRANSFER_BATCH_SIZE_IN_BYTES: UpdateConfiguration.Setting
    TRANSFER_TIME_IN_SECONDS: UpdateConfiguration.Setting
    SETTING_FIELD_NUMBER: _ClassVar[int]
    NEW_VALUE_FIELD_NUMBER: _ClassVar[int]
    setting: UpdateConfiguration.Setting
    new_value: int
    def __init__(self, setting: _Optional[_Union[UpdateConfiguration.Setting, str]] = ..., new_value: _Optional[int] = ...) -> None: ...

class DatabaseMetadata(_message.Message):
    __slots__ = ("table_names",)
    TABLE_NAMES_FIELD_NUMBER: _ClassVar[int]
    table_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, table_names: _Optional[_Iterable[str]] = ...) -> None: ...
