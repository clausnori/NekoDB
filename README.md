# NekoDB
A feature-rich, thread-safe NoSQL database implementation with support for sharding, indexing, and advanced querying capabilities.

## Table of Contents
1. [Getting Started](#getting-started)
2. [Database Features](#database-features)
3. [Data Types and Schema](#data-types-and-schema)
4. [Creating Tables](#creating-tables)
5. [CRUD Operations](#crud-operations)
6. [Querying](#querying)
7. [Performance Features](#performance-features)
8. [Data Integrity](#data-integrity)

## Getting Started

### Installation Requirements
- Python 3.x
- Required packages: lz4, xxhash, zlib

### Basic Usage
```python
from nekodb import DB, QueryCondition

# Initialize database
db = DB(data_dir="./data", cache_size=100000, max_shard_size=1_000_000)

# Create a table
db.create_table("users", {
    "id": {
        "types": ["int"],
        "indexed": True,
        "unique": True,
        "nullable": False,
        "autoincrement": True,
        "shard_key": True
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

# Insert data
db.insert("users", {"name": "John Doe", "age": 30})

# Query data
conditions = [QueryCondition("name", "=", "John Doe")]
results = db.find("users", conditions)
```

## Database Features

### Core Features
1. **Sharding**: Automatic data partitioning across multiple shards
2. **Indexing**: Multiple index types for faster queries
   - Standard indexes
   - String indexes (prefix-based)
   - Bloom filters
3. **Compression**: Multiple compression options
   - LZ4 (default, fast)
   - ZLIB (better compression)
4. **Thread Safety**: Concurrent operations with granular locking
5. **Caching**: In-memory caching for frequent queries
6. **Data Validation**: Type checking and constraint enforcement
7. **Foreign Key References**: Support for relationships between tables

### Performance Optimizations
- Parallel query processing
- Automatic shard rebalancing
- Buffered I/O operations
- Memory-efficient string indexing
- Bloom filters for membership testing

## Data Types and Schema

### Supported Data Types
- `int`: Integer values
- `float`: Floating-point numbers
- `str`: String values
- `bool`: Boolean values
- `datetime`: Date and time values
- `list`: Lists/arrays
- `dict`: Nested objects/dictionaries

### Column Properties
```python
{
    "name": str,              # Column name
    "types": List[str],       # Allowed data types
    "indexed": bool,          # Enable indexing
    "unique": bool,           # Enforce uniqueness
    "nullable": bool,         # Allow NULL values
    "compression": str,       # Compression algorithm
    "shard_key": bool,        # Use for sharding
    "autoincrement": bool,    # Auto-increment values
    "reference": {            # Foreign key reference
        "table": str,
        "field": str
    }
}
```

## Creating Tables

### Table Creation Example
```python
db.create_table("orders", {
    "order_id": {
        "types": ["int"],
        "indexed": True,
        "unique": True,
        "nullable": False,
        "autoincrement": True,
        "shard_key": True
    },
    "customer_id": {
        "types": ["int"],
        "indexed": True,
        "reference": {
            "table": "customers",
            "field": "id"
        }
    },
    "order_date": {
        "types": ["datetime"],
        "indexed": True
    },
    "items": {
        "types": ["list"],
        "nullable": True
    },
    "total_amount": {
        "types": ["float"],
        "nullable": False
    }
})
```

## CRUD Operations

### Insert
```python
# Single insert
db.insert("users", {
    "name": "Jane Smith",
    "age": 25,
    "email": "jane@example.com"
})
```

### Update
```python
# Update records matching conditions
conditions = [
    QueryCondition("age", "<", 30),
    QueryCondition("status", "=", "active")
]
updates = {"category": "young_active"}
db.update("users", conditions, updates)
```

### Delete
```python
# Delete records matching conditions
conditions = [QueryCondition("status", "=", "inactive")]
db.delete("users", conditions)
```

### Find
```python
# Query with conditions and sorting
conditions = [
    QueryCondition("age", ">=", 18),
    QueryCondition("status", "=", "active")
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

## Querying

### Query Conditions
Available operators:
- `=`: Equality
- `>`, `<`, `>=`, `<=`: Comparison
- `!=`: Inequality
- `in`, `not in`: Collection membership
- `contains`: Substring/element containment
- `startswith`, `endswith`: String prefix/suffix
- `regex`: Regular expression matching

### Query Examples
```python
# Complex query with multiple conditions
conditions = [
    QueryCondition("age", ">=", 18),
    QueryCondition("name", "startswith", "J"),
    QueryCondition("status", "in", ["active", "pending"]),
    QueryCondition("email", "regex", r".*@gmail\.com$")
]
results = db.find("users", conditions)

# Query with string indexing
db.find("users", [
    QueryCondition("name", "startswith", "Jo", use_index=True)
])
```

## Performance Features

### Sharding
The database automatically shards data based on the designated shard key:
```python
# Table with sharding configuration
db.create_table("logs", {
    "timestamp": {
        "types": ["datetime"],
        "shard_key": True  # Data will be sharded by timestamp
    },
    "level": {"types": ["str"]},
    "message": {"types": ["str"]}
})
```

### Indexing
```python
# Create table with multiple indexes
db.create_table("products", {
    "id": {
        "types": ["int"],
        "indexed": True,
        "unique": True
    },
    "name": {
        "types": ["str"],
        "indexed": True  # Enables string prefix indexing
    },
    "price": {
        "types": ["float"],
        "indexed": True
    }
})
```

### Compression
```python
# Column-level compression settings
db.create_table("documents", {
    "id": {"types": ["int"]},
    "content": {
        "types": ["str"],
        "compression": "zlib"  # Higher compression for text
    },
    "metadata": {
        "types": ["dict"],
        "compression": "lz4"   # Faster compression for smaller data
    }
})
```

## Data Integrity

### Foreign Key References
```python
# Create tables with relationships
db.create_table("departments", {
    "id": {
        "types": ["int"],
        "unique": True,
        "indexed": True
    },
    "name": {"types": ["str"]}
})

db.create_table("employees", {
    "id": {"types": ["int"]},
    "dept_id": {
        "types": ["int"],
        "reference": {
            "table": "departments",
            "field": "id"
        }
    }
})
```

### Constraints
- Unique constraints
- Not-null constraints
- Foreign key constraints
- Type constraints

### Error Handling
The database throws descriptive exceptions for:
- Table not found
- Invalid data types
- Constraint violations
- Foreign key violations
- Unique key violations
- Missing required fields

### Best Practices
1. Always use appropriate indexes for frequently queried fields
2. Choose shard keys that distribute data evenly
3. Use compression appropriate to your data type
4. Implement proper error handling
5. Close the database properly when done:
```python
try:
    # Database operations
    pass
finally:
    db.close()
```

This documentation provides a comprehensive overview of the database's capabilities. For specific use cases or advanced configurations, refer to the implementation details in the source code.