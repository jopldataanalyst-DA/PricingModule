"""Authentication and authorization routes.

Use case:
    Handles login/logout, JWT token creation, current-user lookup, page access
    checks, admin-only guards, and the special password gate used for sensitive
    item-master changes.
"""

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import jwt
import json
from datetime import datetime, timedelta
from pathlib import Path
from database import hash_password, load_users, save_users
from audit import record_audit_log

SECRET_KEY = "rajnandini_pricing_secret_2025_xK9#mP"
ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 12

router = APIRouter()
security = HTTPBearer()

DATA_DIR = Path(__file__).parent.parent.parent / "Data"

class LoginRequest(BaseModel):
    """Request body accepted by POST /api/auth/login."""
    username: str
    password: str

def create_token(data: dict) -> str:
    """Create a short-lived JWT containing the user's permissions."""
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    """Validate a JWT and return its payload or raise HTTP 401."""
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """FastAPI dependency that returns the authenticated token payload."""
    return decode_token(credentials.credentials)

def require_admin(user=Depends(get_current_user)):
    """FastAPI dependency that allows only admin users."""
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

def is_root_user(user: dict):
    """Return true for the built-in root/admin user that bypasses special auth."""
    return int(user.get("user_id") or user.get("id") or 0) == 1 or user.get("username") == "admin"

def verify_special_password(user: dict, special_password: str | None):
    """Validate the secondary password required for non-root item changes."""
    if is_root_user(user):
        return True
    if not special_password:
        raise HTTPException(status_code=403, detail="Special password is required for item changes")

    user_id = int(user.get("user_id") or user.get("id") or 0)
    username = user.get("username")
    account = next(
        (u for u in load_users() if int(u.get("id") or 0) == user_id or u.get("username") == username),
        None,
    )
    if not account or not account.get("special_password"):
        raise HTTPException(status_code=403, detail="Special password is not configured for this user")
    if account.get("special_password") != hash_password(special_password):
        raise HTTPException(status_code=403, detail="Invalid special password")
    return True

def check_page_access(user: dict, page: str):
    """Raise 403 if the token does not allow access to a named UI page/API."""
    allowed = user.get("allowed_pages", [])
    if isinstance(allowed, str):
        allowed = json.loads(allowed)
    if user.get("role") == "admin":
        return True
    if page not in allowed:
        raise HTTPException(status_code=403, detail=f"No access to page: {page}")
    return True

@router.post("/login")
async def login(req: LoginRequest, request: Request):
    """Authenticate credentials, record login audit, and return a JWT."""
    users = load_users()
    
    for user in users:
        if user.get("username") == req.username and user.get("password") == hash_password(req.password):
            if not user.get("is_active", True):
                raise HTTPException(status_code=401, detail="Account disabled")

            user["last_login"] = datetime.now().isoformat()
            save_users(users)
            
            token_data = {
                "user_id": user.get("id", 0),
                "username": user.get("username"),
                "role": user.get("role", "viewer"),
                "allowed_pages": user.get("allowed_pages", ["item_master"]),
                "column_permissions": user.get("column_permissions", {})
            }
            record_audit_log(token_data, "LOGIN", table_name="users", record_id=user.get("id"), remark="Login successful", request=request)
            return {
                "token": create_token(token_data),
                "user": token_data
            }
    
    record_audit_log({"username": req.username}, "LOGIN_FAILED", table_name="users", remark="Login failed", request=request)
    raise HTTPException(status_code=401, detail="Invalid credentials")

@router.post("/logout")
async def logout(request: Request, user=Depends(get_current_user)):
    """Record logout in the audit log."""
    record_audit_log(user, "LOGOUT", table_name="users", record_id=user.get("user_id"), remark="Logout", request=request)
    return {"message": "Logged out"}

@router.get("/me")
async def get_me(user=Depends(get_current_user)):
    """Return the authenticated token payload for the frontend."""
    return user
