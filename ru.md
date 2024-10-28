# Документация NekoDB

## Содержание
1. [Введение](#введение)
2. [Установка и инициализация](#установка-и-инициализация)
3. [Основные концепции](#основные-концепции)
4. [Типы данных](#типы-данных)
5. [API Reference](#api-reference)
6. [Примеры использования](#примеры-использования)
7. [Работа с индексами](#работа-с-индексами)
8. [Многопоточность](#многопоточность)
9. [Импорт и экспорт данных](#импорт-и-экспорт-данных)

## Введение
NekoDB - это легковесная база данных, реализованная на Python. Она поддерживает:
- Создание таблиц с различными типами данных
- CRUD операции (Create, Read, Update, Delete)
- Индексирование полей для быстрого поиска
- Внешние ключи и ссылочную целостность
- Многопоточный доступ к данным
- Кэширование записей
- Импорт и экспорт данных в JSON

## Установка и инициализация

```python
from nekodb import NekoDB

# Создание экземпляра базы данных
db = NekoDB(data_dir="path/to/data", cache_size=10000)
```

Параметры инициализации:
- `data_dir`: путь к директории для хранения файлов базы данных
- `cache_size`: размер кэша для каждой таблицы (по умолчанию 10000 записей)

## Основные концепции

### Таблицы
Таблица - основная структура для хранения данных. Каждая таблица имеет:
- Имя
- Набор колонок с определенными типами данных
- Метаданные (индексы, количество записей и т.д.)
- Файл для хранения данных

### Колонки
Каждая колонка имеет следующие характеристики:
- Имя
- Список поддерживаемых типов данных
- Флаг nullable (может ли содержать NULL)
- Флаг unique (должны ли значения быть уникальными)
- Флаг indexed (нужно ли индексировать значения)
- Опциональную ссылку на другую таблицу (внешний ключ)

### Условия поиска
Для поиска записей используются объекты `QueryCondition` с операторами:
- `=`: равенство
- `>`: больше
- `<`: меньше
- `>=`: больше или равно
- `<=`: меньше или равно
- `!=`: не равно
- `in`: значение в списке
- `not in`: значение не в списке
- `contains`: содержит подстроку/элемент
- `startswith`: начинается с
- `endswith`: заканчивается на

## Типы данных

NekoDB поддерживает следующие типы данных:

1. `int`: целые числа
   ```python
   'id': {'types': ['int'], 'unique': True}
   ```

2. `float`: числа с плавающей точкой
   ```python
   'price': {'types': ['float']}
   ```

3. `str`: строки
   ```python
   'name': {'types': ['str'], 'nullable': False}
   ```

4. `bool`: логические значения
   ```python
   'active': {'types': ['bool']}
   ```

5. `list`: списки
   ```python
   'tags': {'types': ['list']}
   ```

6. `dict`: словари
   ```python
   'metadata': {'types': ['dict']}
   ```

7. `time`: дата и время
   ```python
   'created_at': {'types': ['time']}
   ```

Поле может поддерживать несколько типов данных:
```python
'value': {'types': ['int', 'float']}
```

## API Reference

### Работа с таблицами

#### create_table
```python
def create_table(self, table_name: str, columns: Dict[str, Dict[str, Any]]) -> None
```
Создает новую таблицу.

Параметры:
- `table_name`: имя таблицы
- `columns`: словарь с описанием колонок

Пример:
```python
db.create_table('users', {
    'id': {'types': ['int'], 'unique': True},
    'name': {'types': ['str'], 'nullable': False},
    'email': {'types': ['str'], 'unique': True},
    'age': {'types': ['int'], 'nullable': True}
})
```

#### drop_table
```python
def drop_table(self, table_name: str) -> None
```
Удаляет таблицу.

Параметры:
- `table_name`: имя таблицы

### Работа с данными

#### insert
```python
def insert(self, table_name: str, record: Dict[str, Any]) -> None
```
Добавляет новую запись в таблицу.

Параметры:
- `table_name`: имя таблицы
- `record`: словарь с данными записи

Пример:
```python
db.insert('users', {
    'id': 1,
    'name': 'John',
    'email': 'john@example.com',
    'age': 30
})
```

#### find
```python
def find(self, table_name: str, conditions: List[QueryCondition] = None,
         order_by: str = None, reverse: bool = False,
         limit: int = None) -> List[Dict[str, Any]]
```
Поиск записей по условиям.

Параметры:
- `table_name`: имя таблицы
- `conditions`: список условий поиска
- `order_by`: поле для сортировки
- `reverse`: обратная сортировка
- `limit`: максимальное количество записей

Пример:
```python
conditions = [
    QueryCondition('age', '>=', 18),
    QueryCondition('name', 'startswith', 'J')
]
users = db.find('users', conditions, order_by='name', limit=10)
```

#### find_one
```python
def find_one(self, table_name: str, conditions: List[QueryCondition]) -> Optional[Dict[str, Any]]
```
Находит первую запись, соответствующую условиям.

#### exists
```python
def exists(self, table_name: str, conditions: List[QueryCondition]) -> bool
```
Проверяет существование записей.

#### count
```python
def count(self, table_name: str, conditions: List[QueryCondition] = None) -> int
```
Подсчитывает количество записей.

#### update
```python
def update(self, table_name: str, conditions: List[QueryCondition],
           updates: Dict[str, Any]) -> int
```
Обновляет записи по условиям.

Параметры:
- `table_name`: имя таблицы
- `conditions`: список условий для поиска записей
- `updates`: словарь с обновляемыми полями

Пример:
```python
conditions = [QueryCondition('id', '=', 1)]
updates = {'age': 31}
updated_count = db.update('users', conditions, updates)
```

#### delete
```python
def delete(self, table_name: str, conditions: List[QueryCondition]) -> int
```
Удаляет записи по условиям.

### Импорт и экспорт

#### export_json
```python
def export_json(self, filename: str) -> None
```
Экспортирует всю базу данных в JSON файл.

#### import_json
```python
def import_json(self, filename: str) -> None
```
Импортирует данные из JSON файла.

## Работа с индексами

Индексы автоматически создаются для полей с флагом `indexed=True`:
```python
db.create_table('posts', {
    'id': {'types': ['int'], 'unique': True},
    'title': {'types': ['str'], 'indexed': True},
    'views': {'types': ['int'], 'indexed': True}
})
```

Индексы ускоряют поиск по условиям равенства:
```python
# Быстрый поиск с использованием индекса
posts = db.find('posts', [QueryCondition('title', '=', 'Hello')])
```

## Многопоточность

NekoDB обеспечивает потокобезопасность через:
1. Блокировки таблиц для операций записи
2. Блокировку кэша
3. Атомарные операции обновления метаданных

Пример безопасного использования в многопоточной среде:
```python
def worker(db, user_id):
    conditions = [QueryCondition('id', '=', user_id)]
    with db._table_lock('users'):
        user = db.find_one('users', conditions)
        if user:
            db.update('users', conditions, {'last_login': datetime.now().isoformat()})
```

## Примеры использования

### Создание связанных таблиц
```python
# Таблица категорий
db.create_table('categories', {
    'id': {'types': ['int'], 'unique': True},
    'name': {'types': ['str'], 'unique': True}
})

# Таблица товаров со ссылкой на категорию
db.create_table('products', {
    'id': {'types': ['int'], 'unique': True},
    'name': {'types': ['str']},
    'category_id': {
        'types': ['int'],
        'reference': {'table': 'categories', 'field': 'id'}
    },
    'price': {'types': ['float']},
    'created_at': {'types': ['time']}
})
```

### Сложный поиск с несколькими условиями
```python
conditions = [
    QueryCondition('price', '>=', 100),
    QueryCondition('price', '<=', 1000),
    QueryCondition('created_at', '>=', '2024-01-01 00:00:00'),
    QueryCondition('category_id', 'in', [1, 2, 3])
]

products = db.find('products', 
                  conditions=conditions,
                  order_by='price',
                  reverse=True,
                  limit=10)
```

### Пакетное обновление
```python
# Повышаем цены на 10% для всех товаров в категории
category_id = 1
conditions = [QueryCondition('category_id', '=', category_id)]
products = db.find('products', conditions)

for product in products:
    update_conditions = [QueryCondition('id', '=', product['id'])]
    new_price = product['price'] * 1.1
    db.update('products', update_conditions, {'price': new_price})
```

## Советы по оптимизации

1. Используйте индексы для часто запрашиваемых полей
2. Ограничивайте размер результатов с помощью `limit`
3. Используйте `find_one` вместо `find`, если нужна одна запись
4. Правильно выбирайте размер кэша при инициализации
5. Используйте блокировки таблиц только когда необходимо
6. Регулярно делайте резервные копии через `export_json`

## Обработка ошибок

NekoDB генерирует следующие исключения:
- `ValueError`: некорректные значения или операции
- `RuntimeError`: ошибки времени выполнения
- `FileNotFoundError`: проблемы с файлами данных
- `json.JSONDecodeError`: ошибки при разборе JSON

Рекомендуется всегда оборачивать операции с базой в try-except:
```python
try:
    db.insert('users', {'id': 1, 'name': 'John'})
except ValueError as e:
    logger.error(f"Ошибка вставки: {e}")
except Exception as e:
    logger.error(f"Неожиданная ошибка: {e}")
```