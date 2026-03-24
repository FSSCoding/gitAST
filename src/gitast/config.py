"""Structured config file diff parser for JSON, YAML, and TOML files."""
import json
from typing import Dict, List, Optional, Tuple

CONFIG_EXTENSIONS = {'.json', '.yaml', '.yml', '.toml'}


def is_config_file(file_path: str) -> bool:
    """Check if a file path is a supported config format."""
    for ext in CONFIG_EXTENSIONS:
        if file_path.endswith(ext):
            return True
    return False


def parse_config(content: str, file_path: str) -> Optional[dict]:
    """Parse a config file into a dict. Returns None on failure."""
    if not content or not content.strip():
        return None

    try:
        if file_path.endswith('.json'):
            return json.loads(content)
        elif file_path.endswith(('.yaml', '.yml')):
            try:
                import yaml
                return yaml.safe_load(content)
            except ImportError:
                return None
        elif file_path.endswith('.toml'):
            try:
                import tomllib
            except ImportError:
                try:
                    import tomli as tomllib
                except ImportError:
                    return None
            return tomllib.loads(content)
    except Exception:
        return None
    return None


def flatten_dict(d: dict, prefix: str = '') -> Dict[str, str]:
    """Flatten a nested dict into dot-separated key paths with string values.

    Example: {"a": {"b": 1}} -> {"a.b": "1"}
    """
    result = {}
    for key, value in d.items():
        full_key = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            result.update(flatten_dict(value, full_key))
        elif isinstance(value, list):
            result[full_key] = json.dumps(value)
        else:
            result[full_key] = str(value) if value is not None else 'null'
    return result


def diff_configs(before: Optional[dict], after: Optional[dict]) -> List[Tuple[str, str, Optional[str], Optional[str]]]:
    """Diff two config dicts and return list of (key_path, change_type, old_value, new_value).

    change_type is one of: 'added', 'modified', 'deleted'
    """
    flat_before = flatten_dict(before) if isinstance(before, dict) else {}
    flat_after = flatten_dict(after) if isinstance(after, dict) else {}

    changes = []

    all_keys = set(flat_before.keys()) | set(flat_after.keys())
    for key in sorted(all_keys):
        old = flat_before.get(key)
        new = flat_after.get(key)

        if old is None and new is not None:
            changes.append((key, 'added', None, new))
        elif old is not None and new is None:
            changes.append((key, 'deleted', old, None))
        elif old != new:
            changes.append((key, 'modified', old, new))

    return changes
