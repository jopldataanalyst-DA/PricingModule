from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import json
from pathlib import Path
from datetime import datetime
from auth import get_current_user, require_admin, hash_password
from database import load_users, save_users

router = APIRouter()

DATA_DIR = Path(__file__).parent.parent.parent / "Data"
USERS_FILE = DATA_DIR / "users.json"
STATS_FILE = DATA_DIR / "stats.json"

class UserUpdate(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    allowed_pages: Optional[List[str]] = None
    column_permissions: Optional[dict] = None
    is_active: Optional[bool] = None

@router.get("/users")
async def get_users(user=Depends(require_admin)):
    users = load_users()
    for u in users:
        u.pop("password", None)
    return users

@router.post("/users")
async def create_user(user_data: UserUpdate, user=Depends(require_admin)):
    users = load_users()
    
    if any(u.get("username") == user_data.username for u in users):
        raise HTTPException(status_code=400, detail="Username already exists")
    
    new_user = {
        "id": max([u.get("id", 0) for u in users]) + 1,
        "username": user_data.username,
        "password": hash_password(user_data.password) if user_data.password else hash_password("password123"),
        "role": user_data.role or "viewer",
        "allowed_pages": user_data.allowed_pages or ["item_master"],
        "column_permissions": user_data.column_permissions or {},
        "is_active": user_data.is_active if user_data.is_active is not None else True
    }
    users.append(new_user)
    save_users(users)
    return {"message": "User created", "id": new_user["id"]}

@router.put("/users/{user_id}")
async def update_user(user_id: int, update: UserUpdate, user=Depends(require_admin)):
    users = load_users()
    
    for u in users:
        if u.get("id") == user_id:
            if update.username:
                u["username"] = update.username
            if update.password:
                u["password"] = hash_password(update.password)
            if update.role:
                u["role"] = update.role
            if update.allowed_pages:
                u["allowed_pages"] = update.allowed_pages
            if update.column_permissions:
                u["column_permissions"] = update.column_permissions
            if update.is_active is not None:
                u["is_active"] = update.is_active
            break
    else:
        raise HTTPException(status_code=404, detail="User not found")
    
    save_users(users)
    return {"message": "User updated"}

@router.delete("/users/{user_id}")
async def delete_user(user_id: int, user=Depends(require_admin)):
    users = load_users()
    
    users = [u for u in users if u.get("id") != user_id]
    save_users(users)
    return {"message": "User deleted"}

@router.get("/stats")
async def get_stats(user=Depends(require_admin)):
    from items import load_items_db
    
    items = load_items_db()
    stats = {
        "total_items": len(items),
        "total_available": sum(int(x.get('available_atp', 0) or 0) for x in items),
        "total_fba": sum(int(x.get('fba_stock', 0) or 0) for x in items),
        "total_sjit": sum(int(x.get('sjit_stock', 0) or 0) for x in items),
        "total_fbf": sum(int(x.get('fbf_stock', 0) or 0) for x in items)
    }
    return stats

@router.get("/import-history")
async def get_import_history(user=Depends(require_admin)):
    IMPORTS_FILE = DATA_DIR / "imports.json"
    if not IMPORTS_FILE.exists():
        return []
    with open(IMPORTS_FILE, 'r') as f:
        return json.load(f)