# NekoDB Documentation

## Table of Contents
1. [Introduction](#introduction)
2. [Installation and Initialization](#installation-and-initialization)
3. [Core Concepts](#core-concepts)
4. [Data Types](#data-types)
5. [API Reference](#api-reference)
6. [Usage Examples](#usage-examples)
7. [Working with Indexes](#working-with-indexes)
8. [Multithreading](#multithreading)
9. [Data Import and Export](#data-import-and-export)

## Introduction

NekoDB is a lightweight database implemented in Python. It supports:
- Table creation with various data types
- CRUD operations (Create, Read, Update, Delete)
- Field indexing for fast searches
- Foreign keys and referential integrity
- Multithreaded data access
- Record caching
- JSON data import/export

## Installation and Initialization

```python
from nekodb import NekoDB

# Create database instance
db = NekoDB(data_dir="path/to/data", cache_size=10000)
```

Initialization parameters:
- `data_dir`: path to the directory for storing database files
- `cache_size`: cache size for each table (default 10000 records)

## Core Concepts

### Tables
A table is the primary structure for data storage. Each table has:
- Name
- Set of columns with defined data types
- Metadata (indexes, record count, etc.)
- Data storage file

### Columns
Each column has the following characteristics:
- Name
- List of supported data types
- Nullable flag (whether it can contain NULL)
- Unique flag (whether values must be unique)
- Indexed flag (whether values should be indexed)
- Optional reference to another table (foreign key)

### Search Conditions
Record searches use `QueryCondition` objects with operators:
- `=`: equality
- `>`: greater than
- `<`: less than
- `>=`: greater than or equal
- `<=`: less than or equal
- `!=`: not equal
- `in`: value in list
- `not in`: value not in list
- `contains`: contains substring/element
- `startswith`: begins with
- `endswith`: ends with

## Data Types

NekoDB supports the following data types:

1. `int`: integers
   ```python
   'id': {'types': ['int'], 'unique': True}
   ```

2. `float`: floating-point numbers
   ```python
   'price': {'types': ['float']}
   ```

3. `str`: strings
   ```python
   'name': {'types': ['str'], 'nullable': False}
   ```

4. `bool`: boolean values
   ```python
   'active': {'types': ['bool']}
   ```

5. `list`: lists
   ```python
   'tags': {'types': ['list']}
   ```

6. `dict`: dictionaries
   ```python
   'metadata': {'types': ['dict']}
   ```

7. `time`: date and time
   ```python
   'created_at': {'types': ['time']}
   ```

A field can support multiple data types:
```python
'value': {'types': ['int', 'float']}
```

## API Reference

### Table Operations

#### create_table
```python
def create_table(self, table_name: str, columns: Dict[str, Dict[str, Any]]) -> None
```
Creates a new table.

Parameters:
- `table_name`: table name
- `columns`: dictionary describing columns

Example:
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
Deletes a table.

Parameters:
- `table_name`: table name

### Data Operations

#### insert
```python
def insert(self, table_name: str, record: Dict[str, Any]) -> None
```
Adds a new record to the table.

Parameters:
- `table_name`: table name
- `record`: dictionary with record data

Example:
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
Search records by conditions.

Parameters:
- `table_name`: table name
- `conditions`: list of search conditions
- `order_by`: field for sorting
- `reverse`: reverse sorting
- `limit`: maximum number of records

Example:
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
Finds the first record matching the conditions.

#### exists
```python
def exists(self, table_name: str, conditions: List[QueryCondition]) -> bool
```
Checks if records exist.

#### count
```python
def count(self, table_name: str, conditions: List[QueryCondition] = None) -> int
```
Counts the number of records.

#### update
```python
def update(self, table_name: str, conditions: List[QueryCondition],
           updates: Dict[str, Any]) -> int
```
Updates records matching conditions.

Parameters:
- `table_name`: table name
- `conditions`: list of conditions to find records
- `updates`: dictionary with fields to update

Example:
```python
conditions = [QueryCondition('id', '=', 1)]
updates = {'age': 31}
updated_count = db.update('users', conditions, updates)
```

#### delete
```python
def delete(self, table_name: str, conditions: List[QueryCondition]) -> int
```
Deletes records matching conditions.

### Import and Export

#### export_json
```python
def export_json(self, filename: str) -> None
```
Exports entire database to a JSON file.

#### import_json
```python
def import_json(self, filename: str) -> None
```
Imports data from a JSON file.

## Working with Indexes

Indexes are automatically created for fields with `indexed=True` flag:
```python
db.create_table('posts', {
    'id': {'types': ['int'], 'unique': True},
    'title': {'types': ['str'], 'indexed': True},
    'views': {'types': ['int'], 'indexed': True}
})
```

Indexes speed up searches with equality conditions:
```python
# Fast search using index
posts = db.find('posts', [QueryCondition('title', '=', 'Hello')])
```

## Multithreading

NekoDB ensures thread safety through:
1. Table locks for write operations
2. Cache locking
3. Atomic metadata update operations

Example of safe usage in a multithreaded environment:
```python
def worker(db, user_id):
    conditions = [QueryCondition('id', '=', user_id)]
    with db._table_lock('users'):
        user = db.find_one('users', conditions)
        if user:
            db.update('users', conditions, {'last_login': datetime.now().isoformat()})
```

## Usage Examples

### Creating Related Tables
```python
# Categories table
db.create_table('categories', {
    'id': {'types': ['int'], 'unique': True},
    'name': {'types': ['str'], 'unique': True}
})

# Products table with category reference
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

### Complex Search with Multiple Conditions
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

### Batch Update
```python
# Increase prices by 10% for all products in category
category_id = 1
conditions = [QueryCondition('category_id', '=', category_id)]
products = db.find('products', conditions)

for product in products:
    update_conditions = [QueryCondition('id', '=', product['id'])]
    new_price = product['price'] * 1.1
    db.update('products', update_conditions, {'price': new_price})
```

## Optimization Tips

1. Use indexes for frequently queried fields
2. Limit result sizes using `limit`
3. Use `find_one` instead of `find` when only one record is needed
4. Choose appropriate cache size during initialization
5. Use table locks only when necessary
6. Regularly backup using `export_json`

## Error Handling

NekoDB generates the following exceptions:
- `ValueError`: invalid values or operations
- `RuntimeError`: runtime errors
- `FileNotFoundError`: data file issues
- `json.JSONDecodeError`: JSON parsing errors

It's recommended to always wrap database operations in try-except:
```python
try:
    db.insert('users', {'id': 1, 'name': 'John'})
except ValueError as e:
    logger.error(f"Insert error: {e}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
```