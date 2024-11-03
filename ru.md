# Документация NoSQL базы данных

## Содержание
1. [Введение](#введение)
2. [Основные концепции](#основные-концепции)
3. [Структура данных](#структура-данных)
4. [API Reference](#api-reference)
5. [Примеры использования](#примеры-использования)

## Введение

NekoDB представляет собой встраиваемую NoSQL систему управления данными с поддержкой:
- Шардирования данных
- Индексирования
- Параллельной обработки
- Сжатия данных
- Ссылочной целостности
- Полнотекстового поиска

## Основные концепции

### Шардирование
NekoDB автоматически разделяет данные на шарды (фрагменты) для оптимальной производительности. Каждый шард содержит подмножество записей, определяемое ключом шардирования.

### Индексирование
Поддерживаются следующие типы индексов:
- Стандартные индексы для быстрого поиска по значению
- Строковые индексы для поиска по префиксу
- Фильтры Блума для быстрой проверки существования значений

### Сжатие
Поддерживаются следующие алгоритмы сжатия:
- LZ4 (по умолчанию)
- ZLIB
- Без сжатия

## Структура данных

### Column (Колонка)
```python
@dataclass
class Column:
    name: str                    # Имя колонки
    types: List[str]            # Допустимые типы данных
    reference: Optional[Reference] # Внешний ключ
    indexed: bool               # Индексировать ли колонку
    unique: bool                # Требование уникальности
    nullable: bool              # Разрешены ли NULL значения
    compression: str            # Алгоритм сжатия
    shard_key: bool            # Использовать как ключ шардирования
    autoincrement: bool        # Автоинкремент
```

### TableMetadata (Метаданные таблицы)
```python
@dataclass
class TableMetadata:
    name: str                   # Имя таблицы
    columns: Dict[str, Column]  # Колонки
    record_count: int          # Количество записей
    indexes: Dict              # Индексы
    file_path: str            # Путь к файлу
    shards: List[TableShard]   # Шарды
    bloom_filters: Dict        # Фильтры Блума
    string_index: Dict        # Строковые индексы
```

## API Reference

### Класс DB

#### Конструктор
```python
def __init__(self, data_dir: str, cache_size: int = 100000,
             max_shard_size: int = 1_000_000,
             worker_threads: int = 8)
```

Параметры:
- `data_dir`: Директория для хранения данных
- `cache_size`: Размер кэша для каждой таблицы
- `max_shard_size`: Максимальный размер шарда
- `worker_threads`: Количество потоков для параллельной обработки

#### Методы

##### create_table
```python
def create_table(self, table_name: str, columns: Dict[str, Dict[str, Any]]) -> None
```
Создает новую таблицу.

Параметры:
- `table_name`: Имя таблицы
- `columns`: Описание колонок

Пример:
```python
db.create_table("users", {
    "id": {
        "types": ["int"],
        "indexed": True,
        "unique": True,
        "nullable": False,
        "autoincrement": True
    },
    "name": {
        "types": ["str"],
        "indexed": True
    },
    "age": {
        "types": ["int"],
        "nullable": True
    }
})
```

##### insert
```python
def insert(self, table_name: str, record: Dict[str, Any]) -> None
```
Вставляет запись в таблицу.

Параметры:
- `table_name`: Имя таблицы
- `record`: Словарь с данными

Пример:
```python
db.insert("users", {
    "name": "Иван",
    "age": 25
})
```

##### find
```python
def find(self, table_name: str, conditions: List[QueryCondition] = None,
         projection: Set[str] = None, order_by: str = None,
         reverse: bool = False, limit: int = None) -> List[Dict[str, Any]]
```
Поиск записей.

Параметры:
- `table_name`: Имя таблицы
- `conditions`: Условия поиска
- `projection`: Набор возвращаемых полей
- `order_by`: Поле для сортировки
- `reverse`: Обратная сортировка
- `limit`: Ограничение количества результатов

Пример:
```python
conditions = [
    QueryCondition("age", ">", 18),
    QueryCondition("name", "startswith", "А")
]
results = db.find("users", conditions, projection={"name", "age"})
```

##### update
```python
def update(self, table_name: str, conditions: List[QueryCondition],
           updates: Dict[str, Any]) -> int
```
Обновляет записи.

Параметры:
- `table_name`: Имя таблицы
- `conditions`: Условия выбора записей
- `updates`: Обновляемые поля

Возвращает количество обновленных записей.

Пример:
```python
conditions = [QueryCondition("age", "<", 18)]
updated = db.update("users", conditions, {"status": "minor"})
```

##### delete
```python
def delete(self, table_name: str, conditions: List[QueryCondition]) -> int
```
Удаляет записи.

Параметры:
- `table_name`: Имя таблицы
- `conditions`: Условия удаления

Возвращает количество удаленных записей.

Пример:
```python
conditions = [QueryCondition("status", "=", "inactive")]
deleted = db.delete("users", conditions)
```

### Класс QueryCondition

#### Конструктор
```python
def __init__(self, field: str, operator: str, value: Any, use_index: bool = True)
```

Параметры:
- `field`: Имя поля
- `operator`: Оператор сравнения
- `value`: Значение
- `use_index`: Использовать ли индекс

Поддерживаемые операторы:
- `=`: Равенство
- `>`: Больше
- `<`: Меньше
- `>=`: Больше или равно
- `<=`: Меньше или равно
- `!=`: Не равно
- `in`: Входит в список
- `not in`: Не входит в список
- `contains`: Содержит подстроку
- `startswith`: Начинается с
- `endswith`: Заканчивается на
- `regex`: Соответствует регулярному выражению

## Примеры использования

### Создание базы данных
```python
from nekodb import DB,QueryCondition
db = DB("data/mydb")
```

### Создание таблицы с внешним ключом
```python
# Создаем таблицу departments
db.create_table("departments", {
    "id": {
        "types": ["int"],
        "indexed": True,
        "unique": True,
        "nullable": False,
        "autoincrement": True
    },
    "name": {
        "types": ["str"],
        "unique": True
    }
})

# Создаем таблицу employees с внешним ключом
db.create_table("employees", {
    "id": {
        "types": ["int"],
        "indexed": True,
        "unique": True,
        "nullable": False,
        "autoincrement": True
    },
    "name": {
        "types": ["str"],
        "indexed": True
    },
    "department_id": {
        "types": ["int"],
        "reference": {
            "table": "departments",
            "field": "id"
        }
    }
})
```

### Использование шардирования
```python
# Создаем таблицу с ключом шардирования
db.create_table("logs", {
    "timestamp": {
        "types": ["datetime"],
        "shard_key": True  # Шардирование по времени
    },
    "level": {
        "types": ["str"],
        "indexed": True
    },
    "message": {
        "types": ["str"]
    }
})
```

### Сложный поиск
```python
# Поиск с несколькими условиями, сортировкой и лимитом
conditions = [
    QueryCondition("age", ">=", 18),
    QueryCondition("status", "=", "active"),
    QueryCondition("name", "startswith", "А")
]

results = db.find(
    table_name="users",
    conditions=conditions,
    projection={"name", "age", "email"},
    order_by="age",
    reverse=True,
    limit=10
)
```

### Эффективное обновление
```python
# Обновление с использованием индекса
conditions = [QueryCondition("status", "=", "pending")]
updates = {
    "status": "processed",
    "processed_at": datetime.now()
}

updated_count = db.update("orders", conditions, updates)
```

### Закрытие базы данных
```python
db.close()
```

## Заключение

NekoDB предоставляет богатый функционал для хранения и обработки данных с поддержкой современных возможностей, таких как шардирование, индексирование и параллельная обработка. При этом она остается простой в использовании благодаря понятному API.

Основные преимущества:
1. Автоматическое шардирование данных
2. Поддержка различных типов индексов
3. Эффективное сжатие данных
4. Параллельная обработка операций
5. Поддержка ссылочной целостности
6. Гибкая система условий поиска
7. Автоматическое управление транзакциями