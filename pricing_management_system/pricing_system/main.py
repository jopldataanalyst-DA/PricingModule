"""FastAPI entrypoint for the pricing management system.

Use case:
    Starts the ERP web application, wires all API routers, serves the HTML UI
    pages, and launches the periodic background pipeline that keeps inventory
    and Amazon pricing data fresh.
"""

from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pathlib import Path

from auth import router as auth_router, get_current_user
from items import router as items_router
from admin import router as admin_router
from logs import router as logs_router
from amazon import router as amazon_router
from sales import router as sales_router
from database import init_users, init_db
from data_pipeline import run_pipeline
import asyncio

app = FastAPI(title="Pricing Management System", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth_router, prefix="/api/auth", tags=["auth"])
app.include_router(items_router, prefix="/api/items", tags=["items"])
app.include_router(admin_router, prefix="/api/admin", tags=["admin"])
app.include_router(logs_router, prefix="/api/logs", tags=["logs"])
app.include_router(amazon_router, prefix="/api/amazon", tags=["amazon"])
app.include_router(sales_router, prefix="/api/sales", tags=["sales"])

# Static files
app.mount("/ui", StaticFiles(directory=r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui"), name="ui")

async def auto_upgrader():
    """Run the full data pipeline every 15 minutes without blocking requests."""
    while True:
        try:
            # Run the pipeline in a thread to avoid blocking the event loop
            await asyncio.to_thread(run_pipeline)
        except Exception as e:
            print(f"Auto-upgrader error: {e}")
        # Wait 15 minutes before the next run
        await asyncio.sleep(900)

@app.on_event("startup")
async def startup():
    """Initialize users/database tables and start the recurring pipeline task."""
    init_db()
    asyncio.create_task(auto_upgrader())

@app.get("/")
async def root():
    """Serve the login page."""
    return FileResponse(r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui\login.html")

@app.get("/dashboard")
async def dashboard():
    """Serve the Item Master dashboard page."""
    return FileResponse(r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui\dashboard.html")

@app.get("/amazon-pricing")
async def amazon_dashboard():
    """Serve the Amazon pricing dashboard page."""
    return FileResponse(r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui\amazon_pricing.html")

@app.get("/errors")
async def errors_page():
    """Serve the operational error tables page."""
    return FileResponse(r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui\errors.html")

@app.get("/admin")
async def admin_page():
    """Serve the admin panel for users, permissions, audit logs, and imports."""
    return FileResponse(r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui\admin.html")

@app.get("/sales")
async def sales_page():
    """Serve the Amazon Sales Analytics dashboard page."""
    return FileResponse(r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui\sales.html")

@app.get("/info")
async def info_page():
    """Serve the visualization guide / documentation page."""
    return FileResponse(r"D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system\ui\info.html")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
