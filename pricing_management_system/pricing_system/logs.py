from fastapi import APIRouter, Depends, Query
import json
from pathlib import Path
from datetime import datetime
from auth import get_current_user, require_admin
from database import hash_password, load_users

router = APIRouter()

DATA_DIR = Path(__file__).parent.parent.parent / "Data"
LOGS_FILE = DATA_DIR / "logs.json"

@router.get("/")
async def get_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    action: str = Query(""),
    username: str = Query(""),
    user=Depends(require_admin)
):
    if not LOGS_FILE.exists():
        return {"logs": [], "total": 0, "page": 1, "total_pages": 1}
    
    with open(LOGS_FILE, 'r') as f:
        logs = json.load(f)
    
    if action:
        logs = [l for l in logs if l.get("action") == action]
    if username:
        logs = [l for l in logs if username.lower() in str(l.get("username", "")).lower()]
    
    total = len(logs)
    start = (page - 1) * page_size
    end = start + page_size
    page_logs = logs[start:end]
    
    return {
        "logs": page_logs,
        "total": total,
        "page": page,
        "total_pages": (total + page_size - 1) // page_size if total else 1
    }

@router.get("/actions")
async def get_actions(user=Depends(require_admin)):
    if not LOGS_FILE.exists():
        return []
    
    with open(LOGS_FILE, 'r') as f:
        logs = json.load(f)
    
    actions = list(set(l.get("action") for l in logs if l.get("action")))
    return sorted(actions)