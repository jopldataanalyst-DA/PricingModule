"""JSON-backed audit and import history helpers.

Use case:
    Provides lightweight persistence for user actions and CSV imports so the
    admin panel can show accountability without needing separate audit tables.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import Request

DATA_DIR = Path(__file__).parent.parent.parent / "Data"
LOGS_FILE = DATA_DIR / "logs.json"
IMPORTS_FILE = DATA_DIR / "imports.json"


def _read_list(path: Path) -> list[dict[str, Any]]:
    """Read a JSON list from disk, returning an empty list on missing/bad data."""
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _write_list(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write a JSON list through a temp file to reduce partial-write risk."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    try:
        temp_path.replace(path)
    except PermissionError:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)
        try:
            temp_path.unlink()
        except OSError:
            pass


def _next_id(rows: list[dict[str, Any]]) -> int:
    """Return the next integer id for a persisted JSON list."""
    return max((int(row.get("id", 0) or 0) for row in rows), default=0) + 1


def load_audit_logs() -> list[dict[str, Any]]:
    """Load audit logs and normalize older entries to the current shape."""
    logs = _read_list(LOGS_FILE)
    for log in logs:
        if "remark" not in log:
            old_values = log.pop("old_values", "")
            new_values = log.pop("new_values", "")
            details = []
            if old_values:
                details.append(f"Old: {old_values}")
            if new_values:
                details.append(f"New: {new_values}")
            log["remark"] = "; ".join(details) or ""
        else:
            log.pop("old_values", None)
            log.pop("new_values", None)
    return logs


def save_audit_logs(logs: list[dict[str, Any]]) -> None:
    """Persist the audit log list."""
    _write_list(LOGS_FILE, logs)


def _json_text(value: Any) -> str:
    """Convert arbitrary audit details into readable text."""
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def record_audit_log(
    user: Optional[dict[str, Any]],
    action: str,
    *,
    table_name: str = "",
    record_id: Any = "",
    old_values: Any = None,
    new_values: Any = None,
    remark: str = "",
    request: Optional[Request] = None,
) -> dict[str, Any]:
    """Create and persist one audit entry for user or system activity."""
    logs = load_audit_logs()
    if not remark:
        parts = []
        if old_values:
            parts.append(f"Old: {_json_text(old_values)}")
        if new_values:
            parts.append(f"New: {_json_text(new_values)}")
        remark = "; ".join(parts)
    entry = {
        "id": _next_id(logs),
        "username": (user or {}).get("username", "system"),
        "user_id": (user or {}).get("user_id") or (user or {}).get("id"),
        "action": action,
        "table_name": table_name,
        "record_id": str(record_id) if record_id is not None else "",
        "remark": remark,
        "ip_address": request.client.host if request and request.client else "",
        "timestamp": datetime.now().isoformat(),
    }
    logs.insert(0, entry)
    save_audit_logs(logs[:5000])
    return entry


def load_import_history() -> list[dict[str, Any]]:
    """Load CSV import history shown in the admin panel."""
    return _read_list(IMPORTS_FILE)


def record_import_history(
    *,
    filename: str,
    imported_by: str,
    total_rows: int,
    new_rows: int = 0,
    updated_rows: int = 0,
    deleted_rows: int = 0,
    skipped_rows: int = 0,
    error_rows: int = 0,
    status: str = "success",
    details: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Create and persist a summary entry for one import operation."""
    imports = load_import_history()
    entry = {
        "id": _next_id(imports),
        "filename": filename,
        "imported_by": imported_by,
        "total_rows": total_rows,
        "new_rows": new_rows,
        "updated_rows": updated_rows,
        "deleted_rows": deleted_rows,
        "skipped_rows": skipped_rows,
        "error_rows": error_rows,
        "status": status,
        "details": details or [],
        "timestamp": datetime.now().isoformat(),
    }
    imports.insert(0, entry)
    _write_list(IMPORTS_FILE, imports[:1000])
    return entry
