# Basic Usage

This guide covers common usage patterns for the library.

## Creating Instances

### Simple Initialization

```python
from my_project import MyClass

obj = MyClass()
```

### With Configuration

```python
from my_project import MyClass, Config

config = Config(debug=True, verbose=True)
obj = MyClass(config=config)
```

## Core Operations

### Operation 1: Process Data

```python
result = obj.process_data(data)
```

!!! note
    This operation is optimized for large datasets.

### Operation 2: Analyze Results

```python
analysis = obj.analyze(result)
print(analysis.summary)
```

!!! warning
    Analysis requires valid input data.

## Working with Multiple Objects

```python
objects = [MyClass() for _ in range(5)]

for obj in objects:
    obj.process()
```

## Performance Tips

- Use batch processing for large datasets
- Enable caching for repeated operations
- Configure appropriate thread pools

## Common Pitfalls

!!! warning "Don't Do This"
    ```python
    # Inefficient
    for item in large_list:
        obj.process(item)

    # Better
    obj.process_batch(large_list)
    ```

## See Also

- [Advanced Features](advanced.md)
- [Configuration Guide](configuration.md)
- [Architecture](../architecture/core.md)
