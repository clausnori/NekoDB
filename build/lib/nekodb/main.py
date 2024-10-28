import re
from typing import Dict, List, Any, Optional, Union, Set, Iterator
from dataclasses import dataclass, asdict
from pathlib import Path
from operator import eq, gt, lt, ge, le, ne
import json
from datetime import datetime
import mmap
import os
import threading
from collections import defaultdict
import logging
from contextlib import contextmanager

#Log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Reference:
    """Description of the foreign key"""
    table: str
    field: str

@dataclass
class Column:
    """Table column description"""
    name: str
    types: List[str]
    reference: Optional[Reference] = None
    indexed: bool = False
    unique: bool = False
    nullable: bool = True

@dataclass
class TableMetadata:
    """Metadata = for index and ..."""
    name: str
    columns: Dict[str, Column]
    record_count: int
    indexes: Dict[str, Dict[Any, List[int]]]  # field -> value -> list of record positions
    file_path: str

class QueryCondition:
    """Condition for searching records"""
    def __init__(self, field: str, operator: str, value: Any):
        self.field = field
        self.operator = operator
        self.value = value
        
        self.operators = {
            '=': eq,
            '>': gt,
            '<': lt,
            '>=': ge,
            '<=': le,
            '!=': ne,
            'in': lambda x, y: x in y,
            'not in': lambda x, y: x not in y,
            'contains': lambda x, y: y in x if isinstance(x, (list, str)) else False,
            'startswith': lambda x, y: x.startswith(y) if isinstance(x, str) else False,
            'endswith': lambda x, y: x.endswith(y) if isinstance(x, str) else False
        }

    def evaluate(self, record: Dict[str, Any]) -> bool:
        """Checks if the record matches the condition"""
        if self.field not in record:
            return False
        
        record_value = record[self.field]
        operator_func = self.operators.get(self.operator)
        
        if operator_func is None:
            raise ValueError(f"Unsupported operator: {self.operator}")
        
        # Convert time strings to datetime objects for comparison
        if isinstance(record_value, str) and isinstance(self.value, str):
            try:
                record_dt = datetime.fromisoformat(record_value)
                value_dt = datetime.fromisoformat(self.value)
                return operator_func(record_dt, value_dt)
            except ValueError:
                pass
            
        try:
            return operator_func(record_value, self.value)
        except TypeError:
            return False

class ChunkIterator:
    """Итератор для чтения записей чанками"""
    def __init__(self, file_path: str, chunk_size: int = 1000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.current_position = 0
        self.file = None
        
    def __enter__(self):
        self.file = open(self.file_path, 'r', encoding='utf-8')
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()
            
    def __iter__(self):
        return self
        
    def __next__(self) -> List[Dict[str, Any]]:
        if not self.file:
            raise RuntimeError("Iterator not initialized. Use 'with' statement.")
            
        records = []
        self.file.seek(self.current_position)
        
        for _ in range(self.chunk_size):
            line = self.file.readline().strip()
            if not line:
                if not records:
                    raise StopIteration
                break
                
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError:
                logger.warning(f"JSON decoding error in position {self.current_position}")
                continue
                
        self.current_position = self.file.tell()
        return records

class DB:
    """Main App"""
    
    def __init__(self, data_dir: str, cache_size: int = 10000):
        """Инициализация базы данных
        
        Args:
            data_dir: Путь к директории с данными
            cache_size: Максимальный размер кэша для каждой таблицы
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.metadata_path = self.data_dir / "meta.json"
        self.tables: Dict[str, TableMetadata] = {}
        self.cache: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)
        self.cache_size = cache_size
        self.cache_lock = threading.Lock()
        self.table_locks: Dict[str, threading.Lock] = {}
        
        self._load_metadata()
        
    def _get_table_lock(self, table_name: str) -> threading.Lock:
        """Получает блокировку для таблицы"""
        if table_name not in self.table_locks:
            self.table_locks[table_name] = threading.Lock()
        return self.table_locks[table_name]
        
    @contextmanager
    def _table_lock(self, table_name: str):
        """Контекстный менеджер для блокировки таблицы"""
        lock = self._get_table_lock(table_name)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def _load_metadata(self) -> None:
        """Загружает метаданные всех таблиц"""
        if not self.metadata_path.exists():
            return
            
        try:
            metadata = json.loads(self.metadata_path.read_text(encoding='utf-8'))
            for table_name, table_meta in metadata.items():
                columns = {
                    name: Column(
                        name=name,
                        types=col['types'],
                        reference=Reference(**col['reference']) if col.get('reference') else None,
                        indexed=col.get('indexed', False),
                        unique=col.get('unique', False),
                        nullable=col.get('nullable', True)
                    )
                    for name, col in table_meta['columns'].items()
                }
                
                self.tables[table_name] = TableMetadata(
                    name=table_name,
                    columns=columns,
                    record_count=table_meta['record_count'],
                    indexes=table_meta['indexes'],
                    file_path=str(self.data_dir / f"{table_name}.tabs")
                )
                
        except Exception as e:
            raise ValueError(f"Ошибка загрузки метаданных: {str(e)}")

    def _save_metadata(self) -> None:
        """Сохраняет метаданные всех таблиц"""
        metadata = {}
        for table_name, table in self.tables.items():
            metadata[table_name] = {
                'columns': {
                    name: {
                        'types': col.types,
                        'reference': asdict(col.reference) if col.reference else None,
                        'indexed': col.indexed,
                        'unique': col.unique,
                        'nullable': col.nullable
                    }
                    for name, col in table.columns.items()
                },
                'record_count': table.record_count,
                'indexes': table.indexes
            }
            
        temp_path = self.metadata_path.with_suffix('.tmp')
        try:
            temp_path.write_text(json.dumps(metadata, indent=2), encoding='utf-8')
            os.replace(str(temp_path), str(self.metadata_path))
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise ValueError(f"Ошибка сохранения метаданных: {str(e)}")

    def _validate_types(self, value: Any, types: List[str]) -> bool:
        """Проверяет соответствие значения типам"""
        if value is None:
            return True
            
        for t in types:
            if t == 'int' and isinstance(value, int):
                return True
            elif t == 'float' and isinstance(value, (int, float)):
                return True
            elif t == 'bool' and isinstance(value, bool):
                return True
            elif t == 'str' and isinstance(value, str):
                return True
            elif t == 'list' and isinstance(value, list):
                return True
            elif t == 'dict' and isinstance(value, dict):
                return True
            elif t == 'time' and isinstance(value, str):
                try:
                    datetime.fromisoformat(value)
                    return True
                except ValueError:
                    return False
                
        return False

    def _convert_value(self, value: Any, types: List[str]) -> Any:
        """Преобразует значение в нужный тип"""
        if value is None:
            return None
            
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
                
        try:
            if 'int' in types:
                return int(value)
            elif 'float' in types:
                return float(value)
            elif 'bool' in types:
                return str(value).lower() == 'true'
            elif 'time' in types:
                try:
                    dt = datetime.fromisoformat(value)
                    return dt.isoformat()
                except ValueError:
                    raise ValueError(f"Invalid time format for value '{value}'. Use ISO format (YYYY-MM-DD HH:MM:SS)")
            elif 'list' in types and isinstance(value, str):
                return json.loads(value)
            elif 'dict' in types and isinstance(value, str):
                return json.loads(value)
            return value
        except (ValueError, json.JSONDecodeError) as e:
            raise ValueError(f"Ошибка преобразования значения '{value}' в типы {types}: {str(e)}")

    def create_table(self, table_name: str, columns: Dict[str, Dict[str, Any]]) -> None:
        """Создает новую таблицу
        
        Args:
            table_name: Имя таблицы
            columns: Словарь с описанием колонок
                    {
                        'name': {
                            'types': ['int', 'str', ...],
                            'reference': {'table': 'table_name', 'field': 'field_name'},
                            'indexed': True/False,
                            'unique': True/False,
                            'nullable': True/False
                        }
                    }
        """
        with self._table_lock(table_name):
            if table_name in self.tables:
                raise ValueError(f"Таблица {table_name} уже существует")
                
            table_columns = {}
            for col_name, col_info in columns.items():
                reference = None
                if 'reference' in col_info:
                    reference = Reference(**col_info['reference'])
                    
                table_columns[col_name] = Column(
                    name=col_name,
                    types=col_info['types'],
                    reference=reference,
                    indexed=col_info.get('indexed', False),
                    unique=col_info.get('unique', False),
                    nullable=col_info.get('nullable', True)
                )
                
            file_path = str(self.data_dir / f"{table_name}.tabs")
            self.tables[table_name] = TableMetadata(
                name=table_name,
                columns=table_columns,
                record_count=0,
                indexes={},
                file_path=file_path
            )
            
            # Создаем индексы для отмеченных полей
            for col_name, column in table_columns.items():
                if column.indexed:
                    self.tables[table_name].indexes[col_name] = {}
                    
            # Создаем пустой файл таблицы
            Path(file_path).touch()
            
            self._save_metadata()

    def drop_table(self, table_name: str) -> None:
        """Удаляет таблицу"""
        with self._table_lock(table_name):
            if table_name not in self.tables:
                raise ValueError(f"Таблица {table_name} не найдена")
                
            # Проверяем ссылки на таблицу
            for t_name, table in self.tables.items():
                if t_name == table_name:
                    continue
                    
                for column in table.columns.values():
                    if column.reference and column.reference.table == table_name:
                        raise ValueError(
                            f"Невозможно удалить таблицу, на неё есть ссылки из {t_name}"
                        )
                        
            # Удаляем файл таблицы
            try:
                Path(self.tables[table_name].file_path).unlink()
            except FileNotFoundError:
                pass
                
            # Удаляем метаданные и кэш
            del self.tables[table_name]
            self.cache.pop(table_name, None)
            self._save_metadata()

    def _update_indexes(self, table: TableMetadata, record: Dict[str, Any], position: int) -> None:
        """Обновляет индексы для записи"""
        for field, index in table.indexes.items():
            if field in record:
                value = record[field]
                if value not in index:
                    index[value] = []
                index[value].append(position)

    def insert(self, table_name: str, record: Dict[str, Any]) -> None:
        """Добавляет новую запись в таблицу"""
        with self._table_lock(table_name):
            if table_name not in self.tables:
                raise ValueError(f"Таблица {table_name} не найдена")
                
            table = self.tables[table_name]
            new_record = {}
            
            # Проверяем и конвертируем данные
            for field, column in table.columns.items():
                value = record.get(field)
                
                # Проверяем nullable
                if value is None and not column.nullable:
                    raise ValueError(f"Поле {field} не может быть NULL")
                    
                if value is not None:
                    # Проверяем типы
                    converted_value = self._convert_value(value, column.types)
                    if not self._validate_types(converted_value, column.types):
                        raise ValueError(
                            f"Значение {value} не соответствует типам {column.types}"
                        )
                        
                    # Проверяем unique
                    if column.unique:
                        conditions = [QueryCondition(field, '=', converted_value)]
                        if self.exists(table_name, conditions):
                            raise ValueError(
                                f"Значение {value} уже существует в поле {field}"
                            )
                            
                    # Проверяем reference
                    if column.reference:
                        self._validate_reference(table_name, column, converted_value)
                        
                    new_record[field] = converted_value
                    
            # Автоинкремент для id
            if 'id' in table.columns and 'autoincrement' in table.columns['id'].types:
                new_record['id'] = table.record_count + 1
                
# Записываем в файл
            with open(table.file_path, 'a', encoding='utf-8') as f:
                line = json.dumps(new_record) + '\n'
                position = f.tell()
                f.write(line)
                
            # Обновляем индексы
            self._update_indexes(table, new_record, position)
            
            # Обновляем метаданные
            table.record_count += 1
            self._save_metadata()
            
            # Добавляем в кэш
            with self.cache_lock:
                self.cache[table_name][position] = new_record
                
                if len(self.cache[table_name]) > self.cache_size:
                    oldest_key = min(self.cache[table_name].keys())
                    self.cache[table_name].pop(oldest_key)

    def _validate_reference(self, table_name: str, column: Column, value: Any) -> bool:
        """Проверяет существование значения в referenced таблице"""
        if not column.reference:
            return True
            
        ref = column.reference
        if ref.table not in self.tables:
            raise ValueError(f"Referenced таблица {ref.table} не существует")
            
        ref_table = self.tables[ref.table]
        if ref.field not in ref_table.columns:
            raise ValueError(
                f"Referenced поле {ref.field} не существует в таблице {ref.table}"
            )
            
        # Проверяем существование значения
        conditions = [QueryCondition(ref.field, '=', value)]
        if not self.exists(ref.table, conditions):
            raise ValueError(
                f"Значение {value} не существует в {ref.table}.{ref.field}"
            )
            
        return True

    def _find_by_index(self, table: TableMetadata, field: str, value: Any) -> List[Dict[str, Any]]:
        """Поиск записей по индексу"""
        if field not in table.indexes:
            return []
            
        positions = table.indexes[field].get(value, [])
        records = []
        
        for pos in positions:
            # Проверяем кэш
            with self.cache_lock:
                if record := self.cache[table.name].get(pos):
                    records.append(record)
                    continue
                    
            # Читаем из файла
            with open(table.file_path, 'r', encoding='utf-8') as f:
                f.seek(pos)
                line = f.readline().strip()
                if line:
                    try:
                        record = json.loads(line)
                        records.append(record)
                        
                        # Добавляем в кэш
                        with self.cache_lock:
                            self.cache[table.name][pos] = record
                    except json.JSONDecodeError:
                        logger.warning(f"Ошибка декодирования JSON в позиции {pos}")
                        
        return records

    def find(self, table_name: str, conditions: List[QueryCondition] = None,
            order_by: str = None, reverse: bool = False,
            limit: int = None) -> List[Dict[str, Any]]:
        """Поиск записей по условиям"""
        if table_name not in self.tables:
            raise ValueError(f"Таблица {table_name} не найдена")
            
        table = self.tables[table_name]
        records = []
        
        # Пробуем использовать индекс для первого условия
        if conditions and conditions[0].operator == '=' and conditions[0].field in table.indexes:
            records = self._find_by_index(table, conditions[0].field, conditions[0].value)
            conditions = conditions[1:]  # Убираем использованное условие
            
        # Если индекс не использовался, читаем все записи
        if not records:
            with ChunkIterator(table.file_path) as iterator:
                for chunk in iterator:
                    records.extend(chunk)
        
        # Применяем оставшиеся условия
        if conditions:
            filtered_records = []
            for record in records:
                if all(condition.evaluate(record) for condition in conditions):
                    filtered_records.append(record)
            records = filtered_records
        
        # Сортировка
        if order_by:
            if order_by not in table.columns:
                raise ValueError(f"Колонка {order_by} не найдена в таблице {table_name}")
            records = sorted(records, key=lambda x: x.get(order_by), reverse=reverse)
        
        # Ограничение количества
        if limit is not None:
            records = records[:limit]
            
        return records

    def find_one(self, table_name: str, conditions: List[QueryCondition]) -> Optional[Dict[str, Any]]:
        """Находит первую запись, соответствующую условиям"""
        results = self.find(table_name, conditions, limit=1)
        return results[0] if results else None

    def exists(self, table_name: str, conditions: List[QueryCondition]) -> bool:
        """Проверяет существование записей"""
        return bool(self.find_one(table_name, conditions))

    def count(self, table_name: str, conditions: List[QueryCondition] = None) -> int:
        """Подсчитывает количество записей"""
        return len(self.find(table_name, conditions))

    def update(self, table_name: str, conditions: List[QueryCondition],
              updates: Dict[str, Any]) -> int:
        """Обновляет записи по условиям"""
        with self._table_lock(table_name):
            if table_name not in self.tables:
                raise ValueError(f"Таблица {table_name} не найдена")
                
            table = self.tables[table_name]
            records_to_update = self.find(table_name, conditions)
            
            if not records_to_update:
                return 0
                
            # Проверяем и конвертируем значения обновления
            converted_updates = {}
            for field, value in updates.items():
                if field not in table.columns:
                    raise ValueError(f"Неизвестная колонка {field}")
                    
                column = table.columns[field]
                
                if value is not None:
                    converted_value = self._convert_value(value, column.types)
                    
                    # Проверяем типы
                    if not self._validate_types(converted_value, column.types):
                        raise ValueError(
                            f"Значение {value} не соответствует типам {column.types}"
                        )
                        
                    # Проверяем unique
                    if column.unique:
                        unique_conditions = [QueryCondition(field, '=', converted_value)]
                        existing = self.find_one(table_name, unique_conditions)
                        if existing and existing.get('id') != updates.get('id'):
                            raise ValueError(
                                f"Значение {value} уже существует в поле {field}"
                            )
                            
                    # Проверяем reference
                    if column.reference:
                        self._validate_reference(table_name, column, converted_value)
                        
                    converted_updates[field] = converted_value
                    
            # Обновляем файл
            updated_count = 0
            temp_file = Path(table.file_path + '.tmp')
            
            try:
                with open(table.file_path, 'r', encoding='utf-8') as src, \
                     open(temp_file, 'w', encoding='utf-8') as dst:
                    for line in src:
                        record = json.loads(line)
                        if any(r.get('id') == record.get('id') for r in records_to_update):
                            record.update(converted_updates)
                            updated_count += 1
                        dst.write(json.dumps(record) + '\n')
                        
                # Заменяем файл
                os.replace(str(temp_file), table.file_path)
                
                # Очищаем кэш
                with self.cache_lock:
                    self.cache[table_name].clear()
                    
                # Перестраиваем индексы
                for field in table.indexes:
                    table.indexes[field].clear()
                    
                with open(table.file_path, 'r', encoding='utf-8') as f:
                    position = 0
                    for line in f:
                        record = json.loads(line)
                        self._update_indexes(table, record, position)
                        position += len(line.encode('utf-8'))
                        
                return updated_count
                
            finally:
                if temp_file.exists():
                    temp_file.unlink()

    def delete(self, table_name: str, conditions: List[QueryCondition]) -> int:
        """Удаляет записи по условиям"""
        with self._table_lock(table_name):
            if table_name not in self.tables:
                raise ValueError(f"Таблица {table_name} не найдена")
                
            table = self.tables[table_name]
            records_to_delete = self.find(table_name, conditions)
            
            if not records_to_delete:
                return 0
                
            # Проверяем ссылочную целостность
            for record in records_to_delete:
                for field, value in record.items():
                    for other_table in self.tables.values():
                        for column in other_table.columns.values():
                            if (column.reference and 
                                column.reference.table == table_name and
                                column.reference.field == field):
                                ref_conditions = [QueryCondition(column.name, '=', value)]
                                if self.exists(other_table.name, ref_conditions):
                                    raise ValueError(
                                        f"Невозможно удалить запись, на неё есть ссылки из {other_table.name}"
                                    )
                                    
            #Del note
            deleted_count = 0
            temp_file = Path(table.file_path + '.tmp')
            
            try:
                with open(table.file_path, 'r', encoding='utf-8') as src, \
                     open(temp_file, 'w', encoding='utf-8') as dst:
                    for line in src:
                        record = json.loads(line)
                        if not any(r.get('id') == record.get('id') for r in records_to_delete):
                            dst.write(line)
                        else:
                            deleted_count += 1
                            
                # replace temp data
                os.replace(str(temp_file), table.file_path)
                
                # update metadata
                table.record_count -= deleted_count
                self._save_metadata()
                
                # clear
                with self.cache_lock:
                    self.cache[table_name].clear()
                    
                # sort index
                for field in table.indexes:
                    table.indexes[field].clear()
                    
                with open(table.file_path, 'r', encoding='utf-8') as f:
                    position = 0
                    for line in f:
                        record = json.loads(line)
                        self._update_indexes(table, record, position)
                        position += len(line.encode('utf-8'))
                        
                return deleted_count
                
            finally:
                if temp_file.exists():
                    temp_file.unlink()

    def export_json(self, filename: str) -> None:
        """Export DB in JSON"""
        data = {}
        for table_name, table in self.tables.items():
            data[table_name] = {
                'columns': {
                    name: {
                        'types': col.types,
                        'reference': asdict(col.reference) if col.reference else None,
                        'indexed': col.indexed,
                        'unique': col.unique,
                        'nullable': col.nullable
                    }
                    for name, col in table.columns.items()
                },
                'records': self.find(table_name)
            }
        Path(filename).write_text(json.dumps(data, indent=2), encoding='utf-8')

    def import_json(self, filename: str) -> None:
        """Import DB from JSON"""
        data = json.loads(Path(filename).read_text(encoding='utf-8'))
        
        for table_name, table_data in data.items():
            if table_name not in self.tables:
                self.create_table(table_name, table_data['columns'])
                
            # Import note
            for record in table_data['records']:
                self.insert(table_name, record)