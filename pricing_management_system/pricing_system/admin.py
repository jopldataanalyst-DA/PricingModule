"""Admin API routes for users, dashboard stats, and import history.

Use case:
    Powers the Admin Panel UI where administrators create users, assign page
    and column permissions, inspect high-level system stats, and review imports.
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from auth import require_admin, hash_password
from database import load_users, save_users, DEFAULT_COLUMN_PERMISSIONS, ADMIN_COLUMN_PERMISSIONS, RESTRICTED_COLUMN_PERMISSIONS, AMAZON_PRICING_COLUMNS
from audit import load_audit_logs, load_import_history, record_audit_log

router = APIRouter()

VALID_ROLES = {"admin", "viewer", "restricted"}
VALID_PAGES = {"item_master", "amazon_pricing", "admin", "logs", "import"}
ITEM_MASTER_COLUMNS = set(DEFAULT_COLUMN_PERMISSIONS["item_master"]["visible"])
AMAZON_PRICING_COLUMNS_SET = set(AMAZON_PRICING_COLUMNS)

class UserUpdate(BaseModel):
    """Payload used for creating and updating local users."""
    username: Optional[str] = None
    password: Optional[str] = None
    special_password: Optional[str] = None
    role: Optional[str] = None
    allowed_pages: Optional[List[str]] = None
    column_permissions: Optional[dict] = None
    is_active: Optional[bool] = None


def _public_user(user: dict) -> dict:
    """Return a user record without password or special-password hashes."""
    data = dict(user)
    data.pop("password", None)
    data.pop("special_password", None)
    return data


def _validate_user_payload(data: UserUpdate, *, creating: bool) -> dict:
    """Validate user-management input before it is saved."""
    username = data.username.strip() if data.username else None
    if creating and not username:
        raise HTTPException(status_code=400, detail="Username is required")
    if username is not None and len(username) < 3:
        raise HTTPException(status_code=400, detail="Username must be at least 3 characters")

    if creating and not data.password:
        raise HTTPException(status_code=400, detail="Password is required")
    if data.password is not None and len(data.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    if data.special_password is not None and data.special_password and len(data.special_password) < 4:
        raise HTTPException(status_code=400, detail="Special password must be at least 4 characters")
    if creating and username != "admin" and not data.special_password:
        raise HTTPException(status_code=400, detail="Special password is required for non-root users")

    role = data.role.lower() if data.role else None
    if role and role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")

    allowed_pages = data.allowed_pages
    if allowed_pages is not None:
        invalid_pages = sorted(set(allowed_pages) - VALID_PAGES)
        if invalid_pages:
            raise HTTPException(status_code=400, detail=f"Invalid pages: {', '.join(invalid_pages)}")

    column_permissions = data.column_permissions
    if column_permissions is not None:
        if not isinstance(column_permissions, dict):
            raise HTTPException(status_code=400, detail="Column permissions must be a dictionary")

        result_perms = {}

        item_master = column_permissions.get("item_master")
        if isinstance(item_master, dict):
            visible = item_master.get("visible", [])
            editable = item_master.get("editable", [])
            invalid_cols = sorted((set(visible) | set(editable)) - ITEM_MASTER_COLUMNS)
            if invalid_cols:
                raise HTTPException(status_code=400, detail=f"Invalid item_master columns: {', '.join(invalid_cols)}")
            result_perms["item_master"] = {"visible": list(visible), "editable": list(editable)}

        amazon_pricing = column_permissions.get("amazon_pricing")
        if isinstance(amazon_pricing, dict):
            visible = amazon_pricing.get("visible", [])
            editable = amazon_pricing.get("editable", [])
            invalid_cols = sorted((set(visible) | set(editable)) - AMAZON_PRICING_COLUMNS_SET)
            if invalid_cols:
                raise HTTPException(status_code=400, detail=f"Invalid amazon_pricing columns: {', '.join(invalid_cols)}")
            result_perms["amazon_pricing"] = {"visible": list(visible), "editable": list(editable)}

        column_permissions = result_perms

    return {
        "username": username,
        "password": data.password,
        "special_password": data.special_password,
        "role": role,
        "allowed_pages": allowed_pages,
        "column_permissions": column_permissions,
        "is_active": data.is_active,
    }


def _default_permissions(role: str) -> dict:
    """Return the default page/column permissions for a role."""
    if role == "admin":
        return ADMIN_COLUMN_PERMISSIONS
    if role == "restricted":
        return RESTRICTED_COLUMN_PERMISSIONS
    return DEFAULT_COLUMN_PERMISSIONS

@router.get("/users")
async def get_users(user=Depends(require_admin)):
    """Return all users in a public-safe shape."""
    users = load_users()
    return [_public_user(u) for u in users]

@router.post("/users")
async def create_user(user_data: UserUpdate, request: Request, user=Depends(require_admin)):
    """Create a new local application user."""
    users = load_users()
    payload = _validate_user_payload(user_data, creating=True)
    
    if any(u.get("username", "").lower() == payload["username"].lower() for u in users):
        raise HTTPException(status_code=400, detail="Username already exists")

    role = payload["role"] or "viewer"
    
    new_user = {
        "id": max([u.get("id", 0) for u in users]) + 1,
        "username": payload["username"],
        "password": hash_password(payload["password"]),
        "role": role,
        "allowed_pages": payload["allowed_pages"] or (["item_master", "amazon_pricing", "admin", "logs", "import"] if role == "admin" else ["item_master"]),
        "column_permissions": payload["column_permissions"] or _default_permissions(role),
        "is_active": payload["is_active"] if payload["is_active"] is not None else True
    }
    if payload["special_password"]:
        new_user["special_password"] = hash_password(payload["special_password"])
    else:
        new_user["special_password"] = ""
    users.append(new_user)
    save_users(users)
    record_audit_log(user, "CREATE_USER", table_name="users", record_id=new_user["id"], remark=f"Created user {new_user['username']}", request=request)
    return {"message": "User created", "id": new_user["id"]}

@router.put("/users/{user_id}")
async def update_user(user_id: int, update: UserUpdate, request: Request, user=Depends(require_admin)):
    """Update role, permissions, passwords, or active status for one user."""
    users = load_users()
    payload = _validate_user_payload(update, creating=False)
    
    for u in users:
        if u.get("id") == user_id:
            old_user = _public_user(u)
            if payload["username"] and any(
                other.get("id") != user_id and other.get("username", "").lower() == payload["username"].lower()
                for other in users
            ):
                raise HTTPException(status_code=400, detail="Username already exists")
            if payload["username"]:
                u["username"] = payload["username"]
            if payload["password"]:
                u["password"] = hash_password(payload["password"])
            if payload["special_password"]:
                u["special_password"] = hash_password(payload["special_password"])
            if payload["role"]:
                u["role"] = payload["role"]
            if payload["allowed_pages"] is not None:
                u["allowed_pages"] = payload["allowed_pages"]
            if payload["column_permissions"] is not None:
                u["column_permissions"] = payload["column_permissions"]
            if payload["is_active"] is not None:
                if user.get("user_id") == user_id and payload["is_active"] is False:
                    raise HTTPException(status_code=400, detail="You cannot deactivate your own admin account")
                u["is_active"] = payload["is_active"]
            break
    else:
        raise HTTPException(status_code=404, detail="User not found")
    
    save_users(users)
    record_audit_log(user, "UPDATE_USER", table_name="users", record_id=user_id, remark=f"Updated user {u.get('username')}", request=request)
    return {"message": "User updated"}

@router.delete("/users/{user_id}")
async def delete_user(user_id: int, request: Request, user=Depends(require_admin)):
    """Delete a user while preventing loss of the last active admin."""
    users = load_users()
    if user.get("user_id") == user_id:
        raise HTTPException(status_code=400, detail="You cannot delete your own account")

    target = next((u for u in users if u.get("id") == user_id), None)
    if target is None:
        raise HTTPException(status_code=404, detail="User not found")
    if target.get("role") == "admin":
        active_admins = [u for u in users if u.get("role") == "admin" and u.get("is_active", True)]
        if len(active_admins) <= 1:
            raise HTTPException(status_code=400, detail="At least one active admin is required")
    
    users = [u for u in users if u.get("id") != user_id]
    save_users(users)
    record_audit_log(user, "DELETE_USER", table_name="users", record_id=user_id, remark=f"Deleted user {target.get('username')}", request=request)
    return {"message": "User deleted"}

@router.get("/stats")
async def get_stats(user=Depends(require_admin)):
    """Return summary counts used by the Admin Panel cards."""
    from items import load_items_db
    
    items = load_items_db()
    users = load_users()
    logs = load_audit_logs()
    imports = load_import_history()
    today = datetime.now().date().isoformat()
    stats = {
        "total_items": len(items),
        "total_users": len(users),
        "active_users": sum(1 for u in users if u.get("is_active", True)),
        "logs_today": sum(1 for log in logs if str(log.get("timestamp", "")).startswith(today)),
        "total_logs": len(logs),
        "total_imports": len(imports),
        "total_available": sum(int(x.get('available_atp', 0) or 0) for x in items),
        "total_fba": sum(int(x.get('fba_stock', 0) or 0) for x in items),
        "total_sjit": sum(int(x.get('sjit_stock', 0) or 0) for x in items),
        "total_fbf": sum(int(x.get('fbf_stock', 0) or 0) for x in items)
    }
    return stats

@router.get("/import-history")
async def get_import_history(user=Depends(require_admin)):
    """Return recorded CSV imports for the Admin Panel history tab."""
    return load_import_history()
