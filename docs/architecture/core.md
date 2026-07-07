# Core API

Core classes and functions documentation.

## MyClass

Main class for basic operations.

```python
from my_project import MyClass
```

### Constructor

```python
MyClass(
    config: Optional[Config] = None,
    name: Optional[str] = None
)
```

**Parameters:**
- `config` - Configuration object
- `name` - Optional name for the instance

**Example:**

```python
from my_project import MyClass, Config

config = Config(debug=True)
obj = MyClass(config=config, name="processor")
```

### Methods

#### `process(data: Any) -> Any`

Process input data and return results.

**Parameters:**
- `data` - Input data to process

**Returns:** Processed result

**Raises:**
- `ValidationError` - If input is invalid
- `ProcessingError` - If processing fails

**Example:**

```python
obj = MyClass()
result = obj.process([1, 2, 3, 4, 5])
print(result)
```

#### `process_batch(items: List[Any]) -> List[Any]`

Process multiple items in batch.

**Parameters:**
- `items` - List of items to process

**Returns:** List of processed results

**Example:**

```python
data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
results = obj.process_batch(data)
```

#### `configure(config: Config) -> None`

Update configuration at runtime.

**Parameters:**
- `config` - New configuration object

**Example:**

```python
from my_project import Config

new_config = Config(debug=False, max_workers=8)
obj.configure(new_config)
```

## Config

Configuration management class.

```python
from my_project import Config
```

### Constructor

```python
Config(
    debug: bool = False,
    verbose: bool = False,
    max_workers: int = 4,
    timeout: int = 30,
    cache_enabled: bool = True,
    log_level: str = "INFO"
)
```

### Class Methods

#### `from_file(path: str) -> Config`

Load configuration from file.

**Parameters:**
- `path` - Path to config file (YAML or JSON)

**Returns:** Config instance

**Example:**

```python
config = Config.from_file("config.yaml")
```

#### `from_env() -> Config`

Load configuration from environment variables.

**Returns:** Config instance

**Example:**

```python
config = Config.from_env()
```

### Properties

- `debug: bool` - Debug mode enabled
- `verbose: bool` - Verbose output
- `max_workers: int` - Number of workers
- `timeout: int` - Operation timeout
- `cache_enabled: bool` - Caching enabled
- `log_level: str` - Logging level

## DataProcessor

Utility class for data processing.

```python
from my_project import DataProcessor
```

### Methods

#### `process(data: Any) -> Any`

Process data using optimized algorithms.

#### `validate(data: Any) -> bool`

Validate input data structure.

## Exceptions

### MyProjectError

Base exception for all library errors.

```python
try:
    obj.process(invalid_data)
except MyProjectError as e:
    print(f"Error: {e}")
```

### ValidationError

Raised when input validation fails.

### ConfigError

Raised when configuration is invalid.

### ProcessingError

Raised when processing fails.

## Type Hints

The library uses type hints extensively. Ensure your Python environment supports them:

```python
from typing import List, Optional
from my_project import MyClass, Config

def process_items(
    obj: MyClass,
    items: List[str]
) -> Optional[dict]:
    return obj.process(items)
```

## See Also

- [Configuration Guide](../guides/configuration.md)
- [Advanced Features](../guides/advanced.md)
