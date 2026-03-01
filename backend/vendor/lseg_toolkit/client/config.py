"""
Configuration management for LSEG API credentials.

Handles loading app keys from optional config files in order of precedence:
1. Local project config: .lseg-config.json (in project root)
2. Global user config: ~/.lseg/config.json
3. No config (use LSEG default app key)
"""

import json
from pathlib import Path

from ..constants import MAX_PARENT_SEARCH_DEPTH


def load_app_key() -> str | None:
    """
    Load LSEG app key from config files.

    Searches for config files in order of precedence:
    1. .lseg-config.json in current/parent directories (local project config)
    2. ~/.lseg/config.json (global user config)
    3. Returns None if no config found (uses LSEG default)

    Config file format (JSON):
    {
        "app_key": "your-app-key-here"
    }

    Returns:
        Optional[str]: App key if found in config, None to use default
    """
    # 1. Check for local project config (.lseg-config.json)
    # Search current directory and parent directories up to project root
    local_config = _find_local_config()
    if local_config:
        app_key = _load_config_file(local_config)
        if app_key:
            return app_key

    # 2. Check for global user config (~/.lseg/config.json)
    global_config = Path.home() / ".lseg" / "config.json"
    if global_config.exists():
        app_key = _load_config_file(global_config)
        if app_key:
            return app_key

    # 3. No config found - use LSEG default
    return None


def _find_local_config() -> Path | None:
    """
    Find local project config file (.lseg-config.json).

    Searches current directory and parent directories up to a reasonable limit.

    Returns:
        Optional[Path]: Path to config file if found, None otherwise
    """
    current = Path.cwd()

    # Search up to MAX_PARENT_SEARCH_DEPTH levels or until we hit root
    for _ in range(MAX_PARENT_SEARCH_DEPTH):
        config_file = current / ".lseg-config.json"
        if config_file.exists():
            return config_file

        # Stop at root or when parent == current (can't go higher)
        if current.parent == current:
            break
        current = current.parent

    return None


def _load_config_file(config_path: Path) -> str | None:
    """
    Load app key from a config file.

    Args:
        config_path: Path to config JSON file

    Returns:
        Optional[str]: App key if found and valid, None otherwise
    """
    try:
        with open(config_path) as f:
            config = json.load(f)

        app_key = config.get("app_key", "").strip()
        if app_key:
            return app_key

    except (OSError, json.JSONDecodeError, KeyError):
        # Silently ignore config file errors - will fall back to default
        pass

    return None


def get_config_paths() -> dict[str, Path]:
    """
    Get paths to config file locations (for documentation/setup purposes).

    Returns:
        dict[str, Path]: Dictionary with 'local' and 'global' config paths
    """
    return {
        "local": Path.cwd() / ".lseg-config.json",
        "global": Path.home() / ".lseg" / "config.json",
    }
