# Rajnandini Pricing Management System
## Complete Project Documentation

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture & Tech Stack](#2-architecture--tech-stack)
3. [Project Structure](#3-project-structure)
4. [Data Flow Diagram](#4-data-flow-diagram)
5. [Authentication & Security](#5-authentication--security)
6. [API Endpoints](#6-api-endpoints)
7. [Database Schema](#7-database-schema)
8. [Frontend Components](#8-frontend-components)
9. [User Roles & Permissions](#9-user-roles--permissions)
10. [Data Pipeline](#10-data-pipeline)
11. [External Integrations](#11-external-integrations)
12. [Default Users](#12-default-users)
13. [Getting Started](#13-getting-started)

---

## 1. Project Overview

**Project Name:** Rajnandini ERP - Pricing Module  
**Purpose:** Enterprise-grade inventory and pricing management system  
**Scale:** Handles 80,000+ SKUs across multiple sales channels  

### Key Features
- Real-time inventory tracking across FBA, SJIT, FBF, and Uniware channels
- Role-based access control with column-level permissions
- Multi-user support with audit logging
- Search, filter, sort, and paginated data browsing
- CSV import/export capabilities
- Auto-sync from Google Sheets every 15 minutes

---

## 2. Architecture & Tech Stack

```
┌─────────────────────────────────────────────────────────────┐
│                        FRONTEND                              │
│                 (Vanilla JS + HTML/CSS)                     │
│              login.html | dashboard.html                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     BACKEND (FastAPI)                        │
│                                                             │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│   │ auth.py  │  │ items.py │  │ admin.py │  │ logs.py  │  │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│                                                             │
│   main.py (Entry Point) │ database.py │ data_pipeline.py   │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌─────────┐    ┌──────────┐    ┌────────────┐
        │  MySQL  │    │  JSON    │    │ Google     │
        │  DB     │    │  Files   │    │ Sheets     │
        └─────────┘    └──────────┘    └────────────┘
```

### Technology Stack
| Layer | Technology |
|-------|------------|
| Backend | FastAPI (Python) |
| Database | MySQL |
| User Data | JSON files |
| Data Processing | Polars |
| Frontend | Vanilla JavaScript, HTML5, CSS3 |
| Authentication | JWT (JSON Web Tokens) |
| External Data | Google Sheets API |

---

## 3. Project Structure

```
D:\VatsalFiles\PricingModule\pricing_management_system\
├── pricing_system/                    # Backend application
│   ├── main.py                       # FastAPI entry point
│   ├── auth.py                       # Authentication & JWT
│   ├── database.py                   # DB & user management
│   ├── items.py                      # Item CRUD operations
│   ├── admin.py                      # Admin operations
│   ├── logs.py                       # Audit logs
│   ├── data_pipeline.py              # Auto-sync from Google Sheets
│   ├── requirements.txt              # Python dependencies
│   ├── start.sh                      # Linux/Mac startup script
│   └── ui/                           # Frontend files
│       ├── login.html                # Login page
│       ├── dashboard.html            # Item Master (main table)
│       └── admin.html                # Admin panel
├── Data/                             # Data directory
│   ├── users.json                    # User credentials & permissions
│   ├── pricing.db                    # SQLite (legacy)
│   ├── ItemMaster.csv                # Master SKU data
│   ├── StockUpdate.csv               # External stock updates
│   └── imports.json                  # Import history
└── README.md                          # Project documentation
```

---

## 4. Data Flow Diagram

### 4.1 User Authentication Flow
```
┌──────────┐      ┌────────────┐      ┌──────────┐      ┌──────────┐
│  User    │      │  Login     │      │  Auth    │      │  JWT     │
│  Browser │ ───► │  Form      │ ───► │  API     │ ───► │  Token   │
└──────────┘      └────────────┘      └──────────┘      └──────────┘
                                                               │
                                                               ▼
                                                          ┌──────────┐
                                                          │ Local    │
                                                          │ Storage  │
                                                          └──────────┘
```

### 4.2 Data Pipeline Flow (Auto-Sync)
```
Google Sheets                    data_pipeline.py              MySQL Database
─────────────                    ────────────────              ──────────────
     │                                  │                            │
     ▼                                  ▼                            ▼
┌─────────────┐                  ┌───────────────┐            ┌───────────┐
│ Stock Data  │ ──HTTP Fetch──►  │ Clean w/      │ ─UPSERT──►  │ stock_items│
│ (CSV URL)   │                  │ Polars        │             │   table   │
└─────────────┘                  └───────────────┘            └───────────┘
                                     │
                                     ▼
                              ┌───────────────┐
                              │ ItemMaster.csv│
                              │ (Join by SKU) │
                              └───────────────┘
```

### 4.3 Dashboard Data Flow
```
┌──────────┐      ┌────────────┐      ┌──────────┐      ┌──────────┐
│ Dashboard│      │ API Fetch  │      │ Items    │      │  MySQL   │
│ JS       │ ───► │ /api/items │ ───► │ Endpoint │ ───► │    DB    │
└──────────┘      └────────────┘      └──────────┘      └──────────┘
     │                   │                    │
     ◄───────────────────┘                    │
     │ (Paginated, filtered results)           │
     ▼                                         ▼
┌──────────────┐                        ┌──────────────┐
│  Render      │                        │ Statistics   │
│  Table       │                        │ (Aggregates) │
└──────────────┘                        └──────────────┘
```

---

## 5. Authentication & Security

### 5.1 Authentication Flow
1. User enters credentials on `login.html`
2. POST request to `/api/auth/login`
3. Server validates against `users.json` (SHA256 hashed passwords)
4. Returns JWT token with user info
5. Token stored in browser `localStorage`
6. All subsequent requests include: `Authorization: Bearer <token>`
7. Token validated on every protected endpoint

### 5.2 Security Configuration
| Setting | Value |
|---------|-------|
| JWT Algorithm | HS256 |
| Secret Key | `rajnandini_pricing_secret_2025_xK9#mP` |
| Token Expiry | 12 hours |
| Password Hashing | SHA256 |

### 5.3 Access Control Levels
- **Admin:** Full access to all pages and features
- **Viewer:** Item Master only (read-only)
- **Restricted:** Item Master with limited columns

---

## 6. API Endpoints

### 6.1 Authentication (`/api/auth`)
| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| POST | `/login` | Login with username/password | No |
| POST | `/logout` | Logout current user | Yes |
| GET | `/me` | Get current user info | Yes |

**Login Request:**
```json
{
  "username": "admin",
  "password": "admin123"
}
```

**Login Response:**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": {
    "user_id": 1,
    "username": "admin",
    "role": "admin",
    "allowed_pages": ["item_master", "admin", "logs", "import"],
    "column_permissions": {}
  }
}
```

### 6.2 Items (`/api/items`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Get paginated items with filters |
| GET | `/{item_id}` | Get single item |
| PUT | `/{item_id}` | Update item |
| DELETE | `/{item_id}` | Delete item (not implemented) |
| POST | `/import` | Import CSV |
| GET | `/columns` | Get column permissions |
| GET | `/filters` | Get filter options |

**GET /api/items Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | int | 1 | Current page number |
| `page_size` | int | 50 | Items per page (max 500) |
| `search` | string | "" | Search by SKU or item name |
| `status_filter` | string | "" | Filter by status |
| `availability` | string | "" | Filter by availability (yes/no) |
| `category` | string | "" | Filter by category |
| `sort_by` | string | "id" | Sort column |
| `sort_dir` | string | "asc" | Sort direction (asc/desc) |

**Response:**
```json
{
  "items": [...],
  "total": 80000,
  "page": 1,
  "page_size": 50,
  "total_pages": 1600,
  "stats": {
    "total_skus": 80000,
    "total_stock": 250000,
    "total_available": 250000,
    "total_fba": 50000,
    "total_sjit": 75000,
    "total_fbf": 125000
  }
}
```

### 6.3 Admin (`/api/admin`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/users` | List all users |
| POST | `/users` | Create new user |
| PUT | `/users/{user_id}` | Update user |
| DELETE | `/users/{user_id}` | Delete user |
| GET | `/stats` | Dashboard statistics |
| GET | `/import-history` | Import history |

### 6.4 Logs (`/api/logs`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Get paginated audit logs |
| GET | `/actions` | Get distinct action types |

---

## 7. Database Schema

### 7.1 MySQL Database: `pricing_module`

**Table: `stock_items`**
```sql
CREATE TABLE stock_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sku_code VARCHAR(255) UNIQUE,
    item_name TEXT,
    size TEXT,
    category VARCHAR(255),
    location TEXT,
    catalog TEXT,
    cost FLOAT,
    price FLOAT,
    mrp FLOAT,
    available_atp INT,
    fba_stock INT,
    fbf_stock INT,
    sjit_stock INT,
    updated TEXT,
    INDEX idx_sku (sku_code),
    INDEX idx_category (category)
);
```

### 7.2 JSON Files

**users.json Structure:**
```json
[
  {
    "id": 1,
    "username": "admin",
    "password": "240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9",
    "role": "admin",
    "allowed_pages": ["item_master", "admin", "logs", "import"],
    "column_permissions": {},
    "is_active": true,
    "last_login": "2026-05-07T10:30:00"
  }
]
```

---

## 8. Frontend Components

### 8.1 Login Page (`login.html`)
- **Purpose:** User authentication
- **Features:**
  - Username/password form
  - JWT token generation
  - Redirect to dashboard on success
  - Auto-redirect if already logged in

### 8.2 Dashboard (`dashboard.html`)
- **Purpose:** Item Master - main data table
- **Features:**
  - Paginated table (50/100/200 per page)
  - Search by SKU or item name
  - Filter by status, availability, category
  - Sort by any column
  - Inline edit (admin only)
  - CSV import (admin only)
  - Real-time stats bar
  - Refresh button

### 8.3 Admin Panel (`admin.html`)
- **Purpose:** User management and system administration
- **Features:**
  - User CRUD (create, read, update, delete)
  - Role and permission management
  - Column-level permissions per user
  - Audit logs viewer
  - Import history

---

## 9. User Roles & Permissions

### 9.1 Role Definitions
| Role | Access | Columns Visible | Can Edit |
|------|--------|-----------------|----------|
| admin | All pages | All 14 columns | Yes (all) |
| viewer | Item Master only | First 5 columns | No |
| restricted | Item Master only | Custom set | Limited |

### 9.2 Available Columns
| Key | Label |
|-----|-------|
| `sku_code` | Master SKU |
| `item_name` | Style ID / Parent SKU |
| `size` | Size |
| `category` | Category |
| `location` | Location |
| `cost` | Cost |
| `price` | Wholesale Price |
| `catalog` | Catalog Name |
| `mrp` | Up Price |
| `available_atp` | Uniware Stock |
| `fba_stock` | FBA |
| `fbf_stock` | FBF |
| `sjit_stock` | SJIT |
| `updated` | Launch Date |

### 9.3 Page Permissions
- `item_master` - Item Master dashboard
- `admin` - Admin panel
- `logs` - Audit logs
- `import` - CSV import feature

---

## 10. Data Pipeline

### 10.1 Auto-Sync Configuration
- **Interval:** Every 15 minutes (900 seconds)
- **Trigger:** Runs automatically on server startup
- **Process:** `asyncio.create_task(auto_upgrader())`

### 10.2 Pipeline Steps
1. **Fetch** stock data from Google Sheets URL
2. **Clean** data using Polars (skip header rows, type casting)
3. **Filter** invalid SKUs (null, empty, "paste here", "uni")
4. **Aggregate** stock by SKU (sum multiple entries)
5. **Join** with ItemMaster.csv on Master SKU
6. **Upsert** to MySQL database (INSERT OR REPLACE)

### 10.3 Stock Sources
| Channel | Column | Source |
|---------|--------|--------|
| Uniware | Uniware Stock | Google Sheets |
| FBA | FBA | Google Sheets |
| FBF | FBF | Google Sheets |
| SJIT | SJIT | Google Sheets |

---

## 11. External Integrations

### 11.1 Google Sheets
**URL:** `https://docs.google.com/spreadsheets/d/e/2PACX-1vTW9CQgk8R7IxKynojzBc0HOB-bMaEHafeBLsAjzc91H9ilRP14PCmdOWvkt8NHzjNeX-HOyjcOwIXh/pub?gid=1527427362&single=true&output=csv`

**Data Format:**
```
uni, uni_stock, fba, fba_stock, fbf, fbf_stock, sjit, sjit_stock
SKU001, 100, SKU001, 50, SKU001, 30, SKU001, 20
SKU002, 200, SKU002, 100, SKU002, 60, SKU002, 40
```

### 11.2 ItemMaster.csv
Required columns:
- `Master SKU` - Unique identifier
- `Style ID / Parent SKU` - Item name
- `Size` - Size variant
- `Category` - Product category
- `Loc` - Location (renamed to Location)

---

## 12. Default Users

| Username | Password | Role | Access |
|----------|----------|------|--------|
| admin | admin123 | admin | Full access |
| vikesh | vikesh123 | viewer | Item Master only |
| hitesh | hitesh123 | restricted | Item Master, limited |

---

## 13. Getting Started

### 13.1 Prerequisites
- Python 3.8+
- MySQL Server
- pip (Python package manager)

### 13.2 Installation
```bash
# Navigate to project directory
cd D:\VatsalFiles\PricingModule\pricing_management_system\pricing_system

# Install dependencies
pip install -r requirements.txt

# Start the server
python main.py
```

### 13.3 Access URLs
| Page | URL |
|------|-----|
| Login | http://localhost:8000/ |
| Dashboard | http://localhost:8000/dashboard |
| Admin Panel | http://localhost:8000/admin |

### 13.4 Configuration
Edit these files to customize:
- `auth.py` - JWT settings, token expiry
- `database.py` - MySQL connection, DB config
- `data_pipeline.py` - Google Sheets URL, sync interval

---

## Quick Reference

### Run Pipeline Manually
```python
from data_pipeline import run_pipeline
run_pipeline()
```

### Check Server Status
```bash
curl http://localhost:8000/
```

### View API Documentation
```
http://localhost:8000/docs (Swagger UI)
http://localhost:8000/redoc (ReDoc)
```

---

*Document generated: May 2026*
*Version: 2.0.0*