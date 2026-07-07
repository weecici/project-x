# API Reference

Complete API documentation for the library.

## Modules

### Core Module (`my_project.core`)

Main classes and functions for core functionality.

- `MyClass` - Primary class for basic operations
- `Config` - Configuration management
- `DataProcessor` - Data processing utilities

[View Core API →](core.md)

### Advanced Module (`my_project.advanced`)

Extended functionality and advanced features.

- `AsyncProcessor` - Asynchronous processing
- `PluginManager` - Plugin system
- `Profiler` - Performance profiling

### Utils Module (`my_project.utils`)

Utility functions and helpers.

- `validate_data()` - Input validation
- `format_output()` - Output formatting
- `Logger` - Logging utilities

### Exceptions

All custom exceptions inherit from `my_project.MyProjectError`.

```python
from my_project import (
    MyProjectError,
    ValidationError,
    ConfigError,
    ProcessingError
)
```

## Version Information

- **Current Version**: 1.0.0
- **Minimum Python**: 3.8
- **Latest Release**: [GitHub Releases](https://github.com/yourusername/my-project/releases)

## Quick Links

- [Installation](../getting-started/installation.md)
- [Quick Start](../getting-started/quickstart.md)
- [Configuration Guide](../guides/configuration.md)
- [Advanced Features](../guides/advanced.md)
