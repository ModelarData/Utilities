from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TableMetadata(_message.Message):
    __slots__ = ("normal_table", "time_series_table")
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
                LOSSLESS: _ClassVar[TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type]
            ABSOLUTE: TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type
            RELATIVE: TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type
            LOSSLESS: TableMetadata.TimeSeriesTableMetadata.ErrorBound.Type
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
    NORMAL_TABLE_FIELD_NUMBER: _ClassVar[int]
    TIME_SERIES_TABLE_FIELD_NUMBER: _ClassVar[int]
    normal_table: TableMetadata.NormalTableMetadata
    time_series_table: TableMetadata.TimeSeriesTableMetadata
    def __init__(self, normal_table: _Optional[_Union[TableMetadata.NormalTableMetadata, _Mapping]] = ..., time_series_table: _Optional[_Union[TableMetadata.TimeSeriesTableMetadata, _Mapping]] = ...) -> None: ...

class Configuration(_message.Message):
    __slots__ = ("ingested_reserved_memory_in_bytes", "uncompressed_reserved_memory_in_bytes", "compressed_reserved_memory_in_bytes", "transfer_batch_size_in_bytes", "segment_size_threshold_in_bytes", "ingestion_threads", "compression_threads", "writer_threads", "wal_enabled", "optimize_target_file_size_in_bytes", "vacuum_retention_period_in_seconds")
    INGESTED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    COMPRESSED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_BATCH_SIZE_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_SIZE_THRESHOLD_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    INGESTION_THREADS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_THREADS_FIELD_NUMBER: _ClassVar[int]
    WRITER_THREADS_FIELD_NUMBER: _ClassVar[int]
    WAL_ENABLED_FIELD_NUMBER: _ClassVar[int]
    OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    VACUUM_RETENTION_PERIOD_IN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ingested_reserved_memory_in_bytes: int
    uncompressed_reserved_memory_in_bytes: int
    compressed_reserved_memory_in_bytes: int
    transfer_batch_size_in_bytes: int
    segment_size_threshold_in_bytes: int
    ingestion_threads: int
    compression_threads: int
    writer_threads: int
    wal_enabled: bool
    optimize_target_file_size_in_bytes: int
    vacuum_retention_period_in_seconds: int
    def __init__(self, ingested_reserved_memory_in_bytes: _Optional[int] = ..., uncompressed_reserved_memory_in_bytes: _Optional[int] = ..., compressed_reserved_memory_in_bytes: _Optional[int] = ..., transfer_batch_size_in_bytes: _Optional[int] = ..., segment_size_threshold_in_bytes: _Optional[int] = ..., ingestion_threads: _Optional[int] = ..., compression_threads: _Optional[int] = ..., writer_threads: _Optional[int] = ..., wal_enabled: _Optional[bool] = ..., optimize_target_file_size_in_bytes: _Optional[int] = ..., vacuum_retention_period_in_seconds: _Optional[int] = ...) -> None: ...

class UpdateConfiguration(_message.Message):
    __slots__ = ("setting", "new_value")
    class Setting(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        INGESTED_RESERVED_MEMORY_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        COMPRESSED_RESERVED_MEMORY_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        TRANSFER_BATCH_SIZE_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        SEGMENT_SIZE_THRESHOLD_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES: _ClassVar[UpdateConfiguration.Setting]
        VACUUM_RETENTION_PERIOD_IN_SECONDS: _ClassVar[UpdateConfiguration.Setting]
    INGESTED_RESERVED_MEMORY_IN_BYTES: UpdateConfiguration.Setting
    UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: UpdateConfiguration.Setting
    COMPRESSED_RESERVED_MEMORY_IN_BYTES: UpdateConfiguration.Setting
    TRANSFER_BATCH_SIZE_IN_BYTES: UpdateConfiguration.Setting
    SEGMENT_SIZE_THRESHOLD_IN_BYTES: UpdateConfiguration.Setting
    OPTIMIZE_TARGET_FILE_SIZE_IN_BYTES: UpdateConfiguration.Setting
    VACUUM_RETENTION_PERIOD_IN_SECONDS: UpdateConfiguration.Setting
    SETTING_FIELD_NUMBER: _ClassVar[int]
    NEW_VALUE_FIELD_NUMBER: _ClassVar[int]
    setting: UpdateConfiguration.Setting
    new_value: int
    def __init__(self, setting: _Optional[_Union[UpdateConfiguration.Setting, str]] = ..., new_value: _Optional[int] = ...) -> None: ...

class NodeMetadata(_message.Message):
    __slots__ = ("url", "mode")
    URL_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    url: str
    mode: str
    def __init__(self, url: _Optional[str] = ..., mode: _Optional[str] = ...) -> None: ...

class ClusterNodes(_message.Message):
    __slots__ = ("nodes",)
    NODES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[NodeMetadata]
    def __init__(self, nodes: _Optional[_Iterable[_Union[NodeMetadata, _Mapping]]] = ...) -> None: ...

class NodeMetrics(_message.Message):
    __slots__ = ("cpu_usage_percentage", "cpu_count", "used_memory_in_bytes", "total_memory_in_bytes", "used_disk_space_in_bytes", "total_disk_space_in_bytes", "ingested_used_memory_in_bytes", "ingested_reserved_memory_in_bytes", "uncompressed_used_memory_in_bytes", "uncompressed_reserved_memory_in_bytes", "compressed_used_memory_in_bytes", "compressed_reserved_memory_in_bytes")
    CPU_USAGE_PERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    CPU_COUNT_FIELD_NUMBER: _ClassVar[int]
    USED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    USED_DISK_SPACE_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DISK_SPACE_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    INGESTED_USED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    INGESTED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    UNCOMPRESSED_USED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    COMPRESSED_USED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    COMPRESSED_RESERVED_MEMORY_IN_BYTES_FIELD_NUMBER: _ClassVar[int]
    cpu_usage_percentage: float
    cpu_count: int
    used_memory_in_bytes: int
    total_memory_in_bytes: int
    used_disk_space_in_bytes: int
    total_disk_space_in_bytes: int
    ingested_used_memory_in_bytes: int
    ingested_reserved_memory_in_bytes: int
    uncompressed_used_memory_in_bytes: int
    uncompressed_reserved_memory_in_bytes: int
    compressed_used_memory_in_bytes: int
    compressed_reserved_memory_in_bytes: int
    def __init__(self, cpu_usage_percentage: _Optional[float] = ..., cpu_count: _Optional[int] = ..., used_memory_in_bytes: _Optional[int] = ..., total_memory_in_bytes: _Optional[int] = ..., used_disk_space_in_bytes: _Optional[int] = ..., total_disk_space_in_bytes: _Optional[int] = ..., ingested_used_memory_in_bytes: _Optional[int] = ..., ingested_reserved_memory_in_bytes: _Optional[int] = ..., uncompressed_used_memory_in_bytes: _Optional[int] = ..., uncompressed_reserved_memory_in_bytes: _Optional[int] = ..., compressed_used_memory_in_bytes: _Optional[int] = ..., compressed_reserved_memory_in_bytes: _Optional[int] = ...) -> None: ...
