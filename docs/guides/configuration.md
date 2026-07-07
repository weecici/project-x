# Configuration

Configure the library to suit your needs.

## Configuration Object

### Using Config Class

```python
from my_project import Config, MyClass

config = Config(
    debug=True,
    verbose=True,
    max_workers=4,
    timeout=30,
    cache_enabled=True,
    log_level="INFO"
)

obj = MyClass(config=config)
```

## Environment Variables

Set configuration via environment variables:

```bash
export MY_PROJECT_DEBUG=true
export MY_PROJECT_LOG_LEVEL=DEBUG
export MY_PROJECT_MAX_WORKERS=8
```

Then load:

```python
from my_project import Config

config = Config.from_env()
```

## Configuration File

### YAML Format

Create `config.yaml`:

```yaml
debug: true
verbose: true
max_workers: 4
timeout: 30
logging:
  level: INFO
  format: json
cache:
  enabled: true
  ttl: 3600
```

Load it:

```python
from my_project import Config

config = Config.from_file("config.yaml")
```

### JSON Format

Create `config.json`:

```json
{
  "debug": true,
  "max_workers": 4,
  "timeout": 30
}
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `debug` | bool | False | Enable debug mode |
| `verbose` | bool | False | Verbose output |
| `max_workers` | int | 4 | Number of worker threads |
| `timeout` | int | 30 | Operation timeout in seconds |
| `cache_enabled` | bool | True | Enable caching |
| `log_level` | str | "INFO" | Logging level |

## Runtime Configuration

Change settings at runtime:

```python
obj = MyClass()

# Modify settings
obj.config.debug = True
obj.config.max_workers = 8
```

## Validation

Configurations are validated automatically:

```python
from my_project import ConfigError

try:
    config = Config(max_workers=-1)  # Invalid
except ConfigError as e:
    print(f"Configuration error: {e}")
```

## Best Practices

1. Use environment variables for deployment
2. Use config files for development
3. Override with command-line arguments
4. Validate config on startup
5. Document all configuration options

## See Also

- [Basic Usage](basic-usage.md)
- [Advanced Features](advanced.md)
