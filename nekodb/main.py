# 1.0
import re
from typing import Dict, List, Any, Optional, Union, Set, Iterator, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import json
import mmap
import os
import threading
from collections import defaultdict
import logging
from contextlib import contextmanager
import zlib
import pickle
from datetime import datetime
import xxhash
import bisect
from concurrent.futures import ThreadPoolExecutor
import lz4.frame
import struct
from typing import BinaryIO
from bisect import bisect_left, bisect_right

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Reference:
    """Foreign key description"""

    table: str
    field: str


@dataclass
class Column:
    """Enhanced table column description"""

    name: str
    types: List[str]
    reference: Optional[Reference] = None
    indexed: bool = False
    unique: bool = False
    nullable: bool = True
    compression: str = "lz4"  # Compression algorithm: none, lz4, zlib
    shard_key: bool = False  # Used for table sharding
    autoincrement: bool = False


@dataclass
class TableShard:
    """Table shard metadata"""

    start_key: Any
    end_key: Any
    file_path: str
    record_count: int


@dataclass
class TableMetadata:
    """Enhanced table metadata"""

    name: str
    columns: Dict[str, Column]
    record_count: int
    indexes: Dict[str, Dict[str, List[int]]]  # field -> value -> list of positions
    file_path: str
    shards: List[TableShard]
    bloom_filters: Dict[str, List[int]]  # field -> bloom filter
    string_index: Dict[str, Dict[str, List[int]]]  # field -> prefix -> positions


class BloomFilter:
    """Bloom filter for efficient membership testing"""

    def __init__(self, size: int = 1000000, hash_count: int = 3):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [0] * size

    def add(self, value: Any):
        for seed in range(self.hash_count):
            h = xxhash.xxh64(str(value), seed=seed).intdigest()
            self.bit_array[h % self.size] = 1

    def contains(self, value: Any) -> bool:
        return all(
            self.bit_array[xxhash.xxh64(str(value), seed=seed).intdigest() % self.size]
            for seed in range(self.hash_count)
        )


class CompressedRecord:
    """Handles record compression and decompression"""

    @staticmethod
    def compress(data: Dict[str, Any], algorithm: str = "lz4") -> bytes:
        serialized = pickle.dumps(data)
        if algorithm == "lz4":
            return lz4.frame.compress(serialized)
        elif algorithm == "zlib":
            return zlib.compress(serialized)
        return serialized

    @staticmethod
    def decompress(data: bytes, algorithm: str = "lz4") -> Dict[str, Any]:
        if algorithm == "lz4":
            decompressed = lz4.frame.decompress(data)
        elif algorithm == "zlib":
            decompressed = zlib.decompress(data)
        else:
            decompressed = data
        return pickle.loads(decompressed)


class StringIndexer:
    """Handles string indexing for faster text search"""

    def __init__(self, min_prefix_length: int = 3):
        self.min_prefix_length = min_prefix_length
        self.index: Dict[str, List[int]] = defaultdict(list)

    def add(self, text: str, position: int):
        if not isinstance(text, str):
            return

        text = text.lower()
        for i in range(self.min_prefix_length, len(text) + 1):
            prefix = text[:i]
            self.index[prefix].append(position)

    def search(self, prefix: str) -> List[int]:
        prefix = prefix.lower()
        if len(prefix) < self.min_prefix_length:
            return []
        return self.index.get(prefix, [])


class RecordBuffer:
    """Buffered record writer/reader"""

    HEADER_FORMAT = "!Q"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, file_path: str, mode: str = "rb", buffer_size: int = 8192):
        self.file_path = file_path
        self.mode = mode
        self.buffer_size = buffer_size
        self.file: Optional[BinaryIO] = None

    def __enter__(self):
        self.file = open(self.file_path, self.mode, buffering=self.buffer_size)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def write_record(self, data: bytes):
        """Writes a record with length prefix"""
        length = len(data)
        header = struct.pack(self.HEADER_FORMAT, length)
        self.file.write(header + data)

    def read_record(self) -> Optional[bytes]:
        """Reads a record with length prefix"""
        header = self.file.read(self.HEADER_SIZE)
        if not header:
            return None
        length = struct.unpack(self.HEADER_FORMAT, header)[0]
        return self.file.read(length)


class QueryCondition:
    """Enhanced query condition with optimization hints"""

    def __init__(self, field: str, operator: str, value: Any, use_index: bool = True):
        self.field = field
        self.operator = operator
        self.value = value
        self.use_index = use_index

        self.operators = {
            "=": lambda x, y: x == y,
            ">": lambda x, y: x > y,
            "<": lambda x, y: x < y,
            ">=": lambda x, y: x >= y,
            "<=": lambda x, y: x <= y,
            "!=": lambda x, y: x != y,
            "in": lambda x, y: x in y,
            "not in": lambda x, y: x not in y,
            "contains": lambda x, y: y in x if isinstance(x, (list, str)) else False,
            "startswith": lambda x, y: x.startswith(y) if isinstance(x, str) else False,
            "endswith": lambda x, y: x.endswith(y) if isinstance(x, str) else False,
            "regex": lambda x, y: (
                bool(re.search(y, x)) if isinstance(x, str) else False
            ),
        }

    def evaluate(self, record: Dict[str, Any]) -> bool:
        """Evaluates condition against a record with type handling"""
        if self.field not in record:
            return False

        record_value = record[self.field]
        operator_func = self.operators.get(self.operator)

        if operator_func is None:
            raise ValueError(f"Unsupported operator: {self.operator}")

        try:
            if isinstance(record_value, str) and isinstance(self.value, str):
                try:
                    record_dt = datetime.fromisoformat(record_value)
                    value_dt = datetime.fromisoformat(self.value)
                    return operator_func(record_dt, value_dt)
                except ValueError:
                    pass

            return operator_func(record_value, self.value)
        except TypeError:
            return False


class DB:
    """Enhanced NoSQL Database with advanced features"""
    hi = """[RUN NEKODB]\n
    VERSION 1.0.1\n
    @Claus_Nori
    """
    def __init__(
        self,
        data_dir: str,
        cache_size: int = 100000,
        max_shard_size: int = 1_000_000,
        worker_threads: int = 8,
    ):
        """Initialize database with enhanced features

        Args:
            data_dir: Data directory path
            cache_size: Maximum cache entries per table
            max_shard_size: Maximum records per shard
            worker_threads: Number of worker threads for parallel operations
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.metadata_path = self.data_dir / "meta.json"
        self.tables: Dict[str, TableMetadata] = {}
        self.cache: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)
        self.cache_size = cache_size
        self.max_shard_size = max_shard_size

        self.worker_pool = ThreadPoolExecutor(max_workers=worker_threads)
        self.cache_lock = threading.Lock()
        self.table_locks: Dict[str, threading.Lock] = {}
        self.shard_locks: Dict[str, Dict[str, threading.Lock]] = defaultdict(dict)

        self._load_metadata()

    def _get_shard_lock(self, table_name: str, shard_path: str) -> threading.Lock:
        """Get lock for specific shard"""
        if shard_path not in self.shard_locks[table_name]:
            self.shard_locks[table_name][shard_path] = threading.Lock()
        return self.shard_locks[table_name][shard_path]

    @contextmanager
    def _multi_shard_lock(self, table_name: str, shard_paths: List[str]):
        """Lock multiple shards for atomic operations"""
        sorted_paths = sorted(shard_paths)
        locks = [self._get_shard_lock(table_name, path) for path in sorted_paths]

        for lock in locks:
            lock.acquire()
        try:
            yield
        finally:
            for lock in reversed(locks):
                lock.release()

    def _create_shard(
        self, table: TableMetadata, start_key: Any, end_key: Any
    ) -> TableShard:
        """Create new shard for table"""
        shard_path = f"{table.file_path}.{len(table.shards)}"
        shard = TableShard(
            start_key=start_key, end_key=end_key, file_path=shard_path, record_count=0
        )
        table.shards.append(shard)
        Path(shard_path).touch()
        return shard

    def _find_shard(self, table: TableMetadata, key: Any) -> Optional[TableShard]:
        """Find appropriate shard for key"""
        for shard in table.shards:
            if (shard.start_key is None or key >= shard.start_key) and (
                shard.end_key is None or key <= shard.end_key
            ):
                return shard
        return None

    def _create_string_index(self, table: TableMetadata, field: str):
        """Create string index for field"""
        if field not in table.string_index:
            table.string_index[field] = {}

        # Build index from existing records
        with ThreadPoolExecutor() as executor:
            futures = []
            for shard in table.shards:
                futures.append(
                    executor.submit(self._build_string_index_shard, table, field, shard)
                )

            for future in futures:
                index_part = future.result()
                for prefix, positions in index_part.items():
                    if prefix not in table.string_index[field]:
                        table.string_index[field][prefix] = []
                    table.string_index[field][prefix].extend(positions)

    def _build_string_index_shard(
        self, table: TableMetadata, field: str, shard: TableShard
    ) -> Dict[str, List[int]]:
        """Build string index for single shard"""
        index = defaultdict(list)
        indexer = StringIndexer()

        with RecordBuffer(shard.file_path) as buf:
            position = 0
            while record_data := buf.read_record():
                record = CompressedRecord.decompress(record_data)
                if field in record and isinstance(record[field], str):
                    indexer.add(record[field], position)
                position = buf.file.tell()

        return indexer.index

    def insert(self, table_name: str, record: Dict[str, Any]) -> None:
        """Insert record into appropriate shard"""
        with self._table_lock(table_name):
            if table_name not in self.tables:
                raise ValueError(f"Table {table_name} not found")

            table = self.tables[table_name]
            for col_name, column in table.columns.items():
                if column.autoincrement and col_name not in record:
                    max_value = self.count(table_name)
                    record[col_name] = max_value + 1

            validated_record = self._validate_record(table, record)

            shard_key_field = next(
                (col.name for col in table.columns.values() if col.shard_key), None
            )

            if shard_key_field:
                shard_key = validated_record.get(shard_key_field)
                shard = self._find_shard(table, shard_key)
                if shard and shard.record_count >= self.max_shard_size:
                    shard = self._create_shard(table, shard.end_key, None)
            else:
                if (
                    not table.shards
                    or table.shards[-1].record_count >= self.max_shard_size
                ):
                    shard = self._create_shard(table, None, None)
                else:
                    shard = table.shards[-1]

            with self._get_shard_lock(table_name, shard.file_path):
                with RecordBuffer(shard.file_path, "ab") as buf:
                    position = buf.file.tell()
                    compressed_record = CompressedRecord.compress(validated_record)
                    buf.write_record(compressed_record)
                    self._update_indexes(table, validated_record, position)
                    shard.record_count += 1

            self._save_metadata()

    def _parallel_search_shard(
        self,
        shard: TableShard,
        conditions: List[QueryCondition],
        projection: Set[str] = None,
    ) -> List[Dict[str, Any]]:
        """Search single shard in parallel"""
        results = []

        with RecordBuffer(shard.file_path) as buf:
            while record_data := buf.read_record():
                record = CompressedRecord.decompress(record_data)

                if all(condition.evaluate(record) for condition in conditions):
                    if projection:
                        results.append(
                            {k: v for k, v in record.items() if k in projection}
                        )
                    else:
                        results.append(record)

        return results
        
    def find(
        self,
        table_name: str,
        conditions: List[QueryCondition] = None,
        projection: Set[str] = None,
        order_by: str = None,
        reverse: bool = False,
        limit: int = None,
    ) -> List[Dict[str, Any]]:
        """Enhanced parallel search with projection, optimizations, and binary search."""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")

        table = self.tables[table_name]
        conditions = conditions or []
        string_conditions = [
            cond
            for cond in conditions
            if cond.operator == "startswith" and cond.field in table.string_index
        ]
        if string_conditions:
            cond = string_conditions[0]
            positions = set()
            for prefix, pos_list in table.string_index[cond.field].items():
                if prefix.startswith(cond.value):
                    positions.update(pos_list)
            if positions:
                conditions = [c for c in conditions if c != cond]
                
        range_conditions = [
            cond
            for cond in conditions
            if cond.operator in (">", ">=", "<", "<=")
            and cond.field in table.sorted_index
        ]
        if range_conditions:
            for cond in range_conditions:
                sorted_values = table.sorted_index[cond.field]
                if cond.operator in (">", ">="):
                    if cond.operator == ">":
                        left_index = bisect_right(sorted_values, cond.value)
                    else:
                        left_index = bisect_left(sorted_values, cond.value)
                    range_positions = sorted_values[left_index:]
                elif cond.operator in ("<", "<="):
                    if cond.operator == "<":
                        right_index = bisect_left(sorted_values, cond.value)
                    else:
                        right_index = bisect_right(sorted_values, cond.value)
                    range_positions = sorted_values[:right_index]
                conditions = [c for c in conditions if c != cond]

        futures = []
        for shard in table.shards:
            futures.append(
                self.worker_pool.submit(
                    self._parallel_search_shard, shard, conditions, projection
                )
            )

        # Collect results
        results = []
        for future in futures:
            results.extend(future.result())

        # Apply sorting if needed
        if order_by:
            results.sort(key=lambda x: x.get(order_by), reverse=reverse)

        # Apply limit if specified
        if limit is not None:
            results = results[:limit]

        return results

    def create_table(self, table_name: str, columns: Dict[str, Dict[str, Any]]) -> None:
        """Create table with enhanced features"""
        with self._table_lock(table_name):
            if table_name in self.tables:
                raise ValueError(f"Table {table_name} already exists")

            # Process columns
            table_columns = {}
            for col_name, col_info in columns.items():
                reference = None
                if "reference" in col_info:
                    reference = Reference(**col_info["reference"])

                table_columns[col_name] = Column(
                    name=col_name,
                    types=col_info["types"],
                    reference=reference,
                    indexed=col_info.get("indexed", False),
                    unique=col_info.get("unique", False),
                    nullable=col_info.get("nullable", True),
                    compression=col_info.get("compression", "lz4"),
                    shard_key=col_info.get("shard_key", False),
                    autoincrement=col_info.get("autoincrement", False),
                )

            # Create table metadata
            file_path = str(self.data_dir / f"{table_name}.db")
            self.tables[table_name] = TableMetadata(
                name=table_name,
                columns=table_columns,
                record_count=0,
                indexes={},
                file_path=file_path,
                shards=[],
                bloom_filters={},
                string_index={},
            )

            # Initialize first shard
            self._create_shard(self.tables[table_name], None, None)

            # Create indexes and bloom filters
            for col_name, column in table_columns.items():
                if column.indexed:
                    self.tables[table_name].indexes[col_name] = {}
                    self.tables[table_name].bloom_filters[col_name] = []

            self._save_metadata()

    def _validate_record(
        self, table: TableMetadata, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate and convert record values"""
        validated = {}

        for field, column in table.columns.items():
            value = record.get(field)

            if value is None:
                if not column.nullable:
                    raise ValueError(f"Field {field} cannot be NULL")
                validated[field] = None
                continue

            # Convert and validate type
            try:
                converted = self._convert_value(value, column.types)
                if not self._validate_types(converted, column.types):
                    raise ValueError(
                        f"Value {value} does not match types {column.types}"
                    )

                # Check unique constraint
                if column.unique:
                    conditions = [QueryCondition(field, "=", converted)]
                    if self.exists(table.name, conditions):
                        raise ValueError(f"Value {value} already exists in {field}")

                # Check reference
                if column.reference:
                    self._validate_reference(table.name, column, converted)

                validated[field] = converted

            except ValueError as e:
                raise ValueError(f"Error validating {field}: {str(e)}")

        return validated

    def update(
        self, table_name: str, conditions: List[QueryCondition], updates: Dict[str, Any]
    ) -> int:
        """Update records with parallel processing"""
        with self._table_lock(table_name):
            if table_name not in self.tables:
                raise ValueError(f"Table {table_name} not found")

            table = self.tables[table_name]

            # Validate updates
            validated_updates = self._validate_record(table, updates)

            # Parallel update across shards
            futures = []
            for shard in table.shards:
                futures.append(
                    self.worker_pool.submit(
                        self._update_shard, table, shard, conditions, validated_updates
                    )
                )

            # Collect results
            total_updated = sum(future.result() for future in futures)

            if total_updated > 0:
                self._save_metadata()

            return total_updated

    def _update_shard(
        self,
        table: TableMetadata,
        shard: TableShard,
        conditions: List[QueryCondition],
        updates: Dict[str, Any],
    ) -> int:
        """Update records in single shard"""
        updated_count = 0
        temp_file = Path(shard.file_path + ".tmp")

        # Process records with locking
        with self._get_shard_lock(table.name, shard.file_path):
            with RecordBuffer(shard.file_path) as read_buf, RecordBuffer(
                str(temp_file), "wb"
            ) as write_buf:

                position = 0
                while record_data := read_buf.read_record():
                    record = CompressedRecord.decompress(record_data)

                    if all(condition.evaluate(record) for condition in conditions):
                        # Update record
                        updated_record = {**record, **updates}
                        record_data = CompressedRecord.compress(updated_record)
                        updated_count += 1

                        # Update indexes
                        self._remove_from_indexes(table, record, position)
                        self._update_indexes(table, updated_record, position)

                    write_buf.write_record(record_data)
                    position = read_buf.file.tell()

        # Replace original file with updated one
        if updated_count > 0:
            temp_file.replace(shard.file_path)
        else:
            temp_file.unlink()

        return updated_count

    def delete(self, table_name: str, conditions: List[QueryCondition]) -> int:
        """Delete records with parallel processing"""
        with self._table_lock(table_name):
            if table_name not in self.tables:
                raise ValueError(f"Table {table_name} not found")

            table = self.tables[table_name]

            # Parallel delete across shards
            futures = []
            for shard in table.shards:
                futures.append(
                    self.worker_pool.submit(
                        self._delete_shard, table, shard, conditions
                    )
                )

            # Collect results
            total_deleted = sum(future.result() for future in futures)

            if total_deleted > 0:
                self._save_metadata()

            return total_deleted

    def _delete_shard(
        self, table: TableMetadata, shard: TableShard, conditions: List[QueryCondition]
    ) -> int:
        """Delete records from single shard"""
        deleted_count = 0
        temp_file = Path(shard.file_path + ".tmp")

        with self._get_shard_lock(table.name, shard.file_path):
            with RecordBuffer(shard.file_path) as read_buf, RecordBuffer(
                str(temp_file), "wb"
            ) as write_buf:

                position = 0
                while record_data := read_buf.read_record():
                    record = CompressedRecord.decompress(record_data)

                    if all(condition.evaluate(record) for condition in conditions):
                        # Remove from indexes
                        self._remove_from_indexes(table, record, position)
                        deleted_count += 1
                    else:
                        write_buf.write_record(record_data)

                    position = read_buf.file.tell()

        if deleted_count > 0:
            temp_file.replace(shard.file_path)
            shard.record_count -= deleted_count
        else:
            temp_file.unlink()

        return deleted_count

    def _update_indexes(
        self, table: TableMetadata, record: Dict[str, Any], position: int
    ) -> None:
        """Update all indexes for a record"""
        for field, column in table.columns.items():
            if not column.indexed or field not in record:
                continue

            value = record[field]

            # Update standard index
            if field in table.indexes:
                if value not in table.indexes[field]:
                    table.indexes[field][value] = []
                table.indexes[field][value].append(position)

            # Update bloom filter
            if field in table.bloom_filters:
                table.bloom_filters[field].append(value)

            # Update string index
            if field in table.string_index and isinstance(value, str):
                indexer = StringIndexer()
                indexer.add(value, position)
                for prefix, positions in indexer.index.items():
                    if prefix not in table.string_index[field]:
                        table.string_index[field][prefix] = []
                    table.string_index[field][prefix].extend(positions)

    def _remove_from_indexes(
        self, table: TableMetadata, record: Dict[str, Any], position: int
    ) -> None:
        """Remove record from all indexes"""
        for field, column in table.columns.items():
            if not column.indexed or field not in record:
                continue

            value = record[field]

            # Remove from standard index
            if field in table.indexes and value in table.indexes[field]:
                table.indexes[field][value].remove(position)
                if not table.indexes[field][value]:
                    del table.indexes[field][value]

            # Remove from string index
            if field in table.string_index and isinstance(value, str):
                indexer = StringIndexer()
                indexer.add(value, position)
                for prefix, positions in indexer.index.items():
                    if prefix in table.string_index[field]:
                        for pos in positions:
                            if pos in table.string_index[field][prefix]:
                                table.string_index[field][prefix].remove(pos)
                        if not table.string_index[field][prefix]:
                            del table.string_index[field][prefix]

    def _convert_value(self, value: Any, types: List[str]) -> Any:
        """Convert value to appropriate type"""
        for type_name in types:
            try:
                if type_name == "int":
                    return int(value)
                elif type_name == "float":
                    return float(value)
                elif type_name == "bool":
                    return bool(value)
                elif type_name == "str":
                    return str(value)
                elif type_name == "datetime":
                    if isinstance(value, str):
                        return datetime.fromisoformat(value)
                    return value
                elif type_name == "list":
                    return list(value)
                elif type_name == "dict":
                    return dict(value)
            except (ValueError, TypeError):
                continue

        raise ValueError(f"Could not convert value {value} to any of types {types}")

    def _validate_types(self, value: Any, types: List[str]) -> bool:
        """Validate value against allowed types"""
        for type_name in types:
            if type_name == "int" and isinstance(value, int):
                return True
            elif type_name == "float" and isinstance(value, (int, float)):
                return True
            elif type_name == "bool" and isinstance(value, bool):
                return True
            elif type_name == "str" and isinstance(value, str):
                return True
            elif type_name == "datetime" and isinstance(value, datetime):
                return True
            elif type_name == "list" and isinstance(value, list):
                return True
            elif type_name == "dict" and isinstance(value, dict):
                return True
        return False

    def _validate_reference(self, table_name: str, column: Column, value: Any) -> None:
        """Validate foreign key reference"""
        if not column.reference:
            return

        ref_table = column.reference.table
        ref_field = column.reference.field

        if ref_table not in self.tables:
            raise ValueError(f"Referenced table {ref_table} not found")

        conditions = [QueryCondition(ref_field, "=", value)]
        if not self.exists(ref_table, conditions):
            raise ValueError(
                f"Referenced value {value} not found in {ref_table}.{ref_field}"
            )

    def exists(self, table_name: str, conditions: List[QueryCondition]) -> bool:
        """Check if any record matches the conditions"""
        results = self.find(table_name, conditions, limit=1)
        return len(results) > 0

    def count(self, table_name: str, conditions: List[QueryCondition] = None) -> int:
        """Count records matching conditions"""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")

        conditions = conditions or []
        total = 0

        futures = []
        for shard in self.tables[table_name].shards:
            futures.append(
                self.worker_pool.submit(self._count_shard, shard, conditions)
            )

        return sum(future.result() for future in futures)

    def _count_shard(self, shard: TableShard, conditions: List[QueryCondition]) -> int:
        """Count matching records in single shard"""
        count = 0

        with RecordBuffer(shard.file_path) as buf:
            while record_data := buf.read_record():
                record = CompressedRecord.decompress(record_data)
                if all(condition.evaluate(record) for condition in conditions):
                    count += 1

        return count

    def close(self):
        """Clean up resources"""
        self._save_metadata()
        self.worker_pool.shutdown()

        self.cache.clear()
        self.table_locks.clear()
        self.shard_locks.clear()

    def _save_metadata(self):
        """Save database metadata to disk"""
        metadata = {
            "tables": {
                name: {
                    "name": table.name,
                    "columns": {
                        col.name: {
                            "types": col.types,
                            "reference": (
                                asdict(col.reference) if col.reference else None
                            ),
                            "indexed": col.indexed,
                            "unique": col.unique,
                            "nullable": col.nullable,
                            "compression": col.compression,
                            "shard_key": col.shard_key,
                            "autoincrement": col.autoincrement,
                        }
                        for col in table.columns.values()
                    },
                    "file_path": table.file_path,
                    "record_count": table.record_count,
                    "shards": [
                        {
                            "start_key": shard.start_key,
                            "end_key": shard.end_key,
                            "file_path": shard.file_path,
                            "record_count": shard.record_count,
                        }
                        for shard in table.shards
                    ],
                }
                for name, table in self.tables.items()
            }
        }

        with open(self.metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def _load_metadata(self):
        """Load database metadata from disk"""
        if not self.metadata_path.exists():
            return

        with open(self.metadata_path) as f:
            metadata = json.load(f)

        for table_name, table_data in metadata["tables"].items():
            columns = {}
            for col_name, col_data in table_data["columns"].items():
                reference = None
                if col_data["reference"]:
                    reference = Reference(**col_data["reference"])

                columns[col_name] = Column(
                    name=col_name,
                    types=col_data["types"],
                    reference=reference,
                    indexed=col_data["indexed"],
                    unique=col_data["unique"],
                    nullable=col_data["nullable"],
                    compression=col_data["compression"],
                    shard_key=col_data["shard_key"],
                    autoincrement=col_data["autoincrement"],
                )

            shards = [TableShard(**shard_data) for shard_data in table_data["shards"]]

            self.tables[table_name] = TableMetadata(
                name=table_data["name"],
                columns=columns,
                record_count=table_data["record_count"],
                indexes={},
                file_path=table_data["file_path"],
                shards=shards,
                bloom_filters={},
                string_index={},
            )

            # Initialize locks
            self.table_locks[table_name] = threading.Lock()
            for shard in shards:
                self.shard_locks[table_name][shard.file_path] = threading.Lock()

    @contextmanager
    def _table_lock(self, table_name: str):
        """Context manager for table-level locking"""
        if table_name not in self.table_locks:
            self.table_locks[table_name] = threading.Lock()

        with self.table_locks[table_name]:
            yield
