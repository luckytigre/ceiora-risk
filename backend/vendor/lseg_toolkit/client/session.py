"""
Session management for LSEG API.

Handles opening, closing, and context management for LSEG data sessions.
"""

import logging
import os
import re
import warnings
from pathlib import Path
from typing import Any

import lseg.data as rd

from ..exceptions import SessionError
from .config import load_app_key

logger = logging.getLogger(__name__)

# Suppress FutureWarnings from LSEG library about pandas replace() downcasting
# These warnings come from lseg.data._tools._dataframe.py:192 and are not in our control
warnings.filterwarnings(
    "ignore", category=FutureWarning, module="lseg.data._tools._dataframe"
)

_WORKSPACE_BASE_URL_ENV = "LSEG_WORKSPACE_BASE_URL"
_WORKSPACE_PORT_ENV = "LSEG_WORKSPACE_PORT"
_WORKSPACE_LOG_ROOT_ENV = "LSEG_WORKSPACE_LOG_ROOT"
_SESSION_TIMEOUT_ENV = "LSEG_SESSION_TIMEOUT_SECONDS"
_DEFAULT_SESSION_TIMEOUT_SECONDS = 120
_WORKSPACE_LOG_ROOT = Path.home() / "Library/Application Support/Refinitiv/Refinitiv Workspace Logs"
_WORKSPACE_PORT_RE = re.compile(r"API Proxy is listening to port:\s*(\d+)")


def _resolve_session_timeout_seconds() -> int:
    raw = str(os.getenv(_SESSION_TIMEOUT_ENV, str(_DEFAULT_SESSION_TIMEOUT_SECONDS))).strip()
    try:
        return max(20, int(raw))
    except Exception:
        return _DEFAULT_SESSION_TIMEOUT_SECONDS


def _workspace_log_roots() -> list[Path]:
    roots: list[Path] = []
    env_root = str(os.getenv(_WORKSPACE_LOG_ROOT_ENV, "")).strip()
    if env_root:
        roots.append(Path(env_root).expanduser())
    roots.append(_WORKSPACE_LOG_ROOT)
    deduped: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        try:
            resolved = root.expanduser().resolve()
        except OSError:
            continue
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(resolved)
    return deduped


def _workspace_log_directories() -> list[Path]:
    directories: list[Path] = []
    for root in _workspace_log_roots():
        if not root.exists():
            continue
        directories.extend(p for p in root.glob("Desktop.*") if p.is_dir())
    return sorted(
        directories,
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def _latest_workspace_api_port_from_logs() -> int | None:
    for log_dir in _workspace_log_directories():
        node_logs = sorted(
            log_dir.glob("node-sxs.*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for log_path in node_logs:
            try:
                matches = _WORKSPACE_PORT_RE.findall(log_path.read_text(errors="ignore"))
            except OSError:
                continue
            if matches:
                try:
                    return int(matches[-1])
                except Exception:
                    continue
    return None


def resolve_workspace_base_url() -> str | None:
    explicit_base_url = str(os.getenv(_WORKSPACE_BASE_URL_ENV, "")).strip()
    if explicit_base_url:
        return explicit_base_url
    explicit_port = str(os.getenv(_WORKSPACE_PORT_ENV, "")).strip()
    if explicit_port:
        try:
            return f"http://localhost:{int(explicit_port)}"
        except Exception:
            logger.warning("Ignoring invalid %s=%r", _WORKSPACE_PORT_ENV, explicit_port)
    detected_port = _latest_workspace_api_port_from_logs()
    if detected_port is None:
        return None
    return f"http://localhost:{detected_port}"


def _desktop_definition_type():
    try:
        from lseg.data.session.desktop import Definition
    except Exception:
        return None
    return Definition


def _default_session_matches(session: Any) -> bool:
    if session is None:
        return False
    try:
        return rd.session.get_default() is session
    except Exception:
        return False


def _close_default_session() -> None:
    try:
        rd.close_session()
    except Exception as exc:
        logger.warning("Error clearing default LSEG session: %s", exc)


def _legacy_open_session(*, app_key: str | None = None):
    kwargs_candidates = [
        {"name": "desktop.workspace", "app_key": app_key},
        {"app_key": app_key},
        {},
    ]
    last_error: Exception | None = None
    for raw_kwargs in kwargs_candidates:
        kwargs = {key: value for key, value in raw_kwargs.items() if value is not None}
        try:
            return rd.open_session(**kwargs)
        except TypeError as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return rd.open_session()


def open_managed_session(*, app_key: str | None = None):
    base_url = resolve_workspace_base_url()
    timeout_seconds = _resolve_session_timeout_seconds()
    definition_type = _desktop_definition_type()
    if definition_type is None:
        if base_url:
            logger.warning(
                "Ignoring %s because desktop Definition is unavailable in this LSEG SDK layout",
                _WORKSPACE_BASE_URL_ENV,
            )
        return _legacy_open_session(app_key=app_key)

    session = definition_type(name="workspace", app_key=app_key).get_session()
    if base_url:
        session.config["sessions.desktop.workspace.base-url"] = base_url
    if hasattr(session, "set_timeout"):
        session.set_timeout(timeout_seconds)
    try:
        session.open()
    except Exception:
        try:
            session.close()
        except Exception:
            pass
        raise
    try:
        rd.session.set_default(session)
    except Exception:
        try:
            session.close()
        except Exception:
            pass
        raise
    return session


def close_managed_session(session) -> None:
    if session is None:
        _close_default_session()
        return
    clear_default = _default_session_matches(session)
    try:
        session.close()
    except Exception as exc:
        logger.warning("Error closing managed LSEG session: %s", exc)
    if clear_default:
        _close_default_session()


class SessionManager:
    """
    Manages LSEG Data session lifecycle.

    Provides context manager support for automatic session cleanup.
    """

    def __init__(self, auto_open: bool = True):
        """
        Initialize session manager.

        Args:
            auto_open: Automatically open LSEG session (default: True)
        """
        self._session_opened = False
        self._session = None
        if auto_open:
            self.open_session()

    def open_session(self):
        """
        Open LSEG Data session.

        Automatically loads app key from config files (if present):
        1. .lseg-config.json (local project config)
        2. ~/.lseg/config.json (global user config)
        3. Falls back to LSEG default if no config found

        Requires LSEG Workspace Desktop to be running and authenticated.
        WSL2 users must have mirrored networking enabled (see WSL_SETUP.md).

        To create a config file with your app key, run:
            uv run lseg-setup
        """
        if not self._session_opened:
            try:
                app_key = load_app_key()
                self._session = open_managed_session(app_key=app_key)
                self._session_opened = True
            except Exception as e:
                error_msg = (
                    f"Failed to open LSEG session: {e}\n\n"
                    "Possible solutions:\n"
                    "1. Ensure LSEG Workspace Desktop is running and logged in\n"
                    "2. Create an app key in LSEG Workspace and configure it:\n"
                    "   Run: uv run lseg-setup\n"
                    "3. On WSL2: Ensure mirrored networking is enabled (see WSL_SETUP.md)"
                )
                raise SessionError(error_msg) from e

    def close_session(self):
        """Close LSEG Data session."""
        if self._session_opened:
            try:
                close_managed_session(self._session)
                self._session = None
                self._session_opened = False
            except Exception as e:
                logger.warning(f"Error closing LSEG session: {e}")

    def __enter__(self):
        """Context manager entry."""
        self.open_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_session()

    def __del__(self):
        """Cleanup on deletion."""
        self.close_session()

    @property
    def is_open(self) -> bool:
        """Check if session is currently open."""
        return self._session_opened
