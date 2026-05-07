from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import jwt
import json
from datetime import datetime, timedelta
from pathlib import Path
from database import hash_password, load_users, save_users

SECRET_KEY = "rajnandini_pricing_secret_2025_xK9#mP"
ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 12

router = APIRouter()
security = HTTPBearer()

DATA_DIR = Path(__file__).parent.parent.parent / "Data"

class LoginRequest(BaseModel):
    username: str
    password: str

def create_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return decode_token(credentials.credentials)

def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

def check_page_access(user: dict, page: str):
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
    users = load_users()
    
    for user in users:
        if user.get("username") == req.username and user.get("password") == hash_password(req.password):
            if not user.get("is_active", True):
                raise HTTPException(status_code=401, detail="Account disabled")
            
            token_data = {
                "user_id": user.get("id", 0),
                "username": user.get("username"),
                "role": user.get("role", "viewer"),
                "allowed_pages": user.get("allowed_pages", ["item_master"]),
                "column_permissions": user.get("column_permissions", {})
            }
            return {
                "token": create_token(token_data),
                "user": token_data
            }
    
    raise HTTPException(status_code=401, detail="Invalid credentials")

@router.post("/logout")
async def logout(request: Request, user=Depends(get_current_user)):
    return {"message": "Logged out"}

@router.get("/me")
async def get_me(user=Depends(get_current_user)):
    return user