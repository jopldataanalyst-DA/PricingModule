# Rajnandini Pricing Management System

A production-grade FastAPI + vanilla JS pricing & inventory management system.

## Quick Start

```bash
cd pricing_system
pip install -r requirements.txt
python main.py
```
Open: **http://localhost:8000**

---

## Default Credentials

| User   | Password   | Role       | Access                        |
|--------|------------|------------|-------------------------------|
| admin  | admin123   | Admin      | All pages + full CRUD         |
| vikesh | vikesh123  | Viewer     | Item Master (view only)       |
| hitesh | hitesh123  | Restricted | Item Master (limited columns) |

---

## Project Structure

```
pricing_system/
├── main.py           # FastAPI app entry point
├── auth.py           # JWT authentication + login/logout
├── items.py          # Stock items CRUD + CSV import
├── admin.py          # User management + admin stats
├── logs.py           # Audit log viewer
├── database.py       # SQLite setup + schema
├── requirements.txt
├── start.sh          # Quick start script
├── data/
│   ├── pricing.db    # SQLite database (auto-created)
│   └── stock_update.csv  # Initial stock data
└── ui/
    ├── login.html    # Login page
    ├── dashboard.html # Item Master page
    └── admin.html    # Admin panel
```

---

## Features

### Authentication & Authorization
- JWT-based login with 12-hour token expiry
- Role-based access: `admin`, `viewer`, `restricted`
- Per-user page access control
- Per-column visibility & editability control

### Item Master
- Paginated table (50/100/200 rows)
- Search: SKU, item name, brand, color
- Filters: Status, Availability, Category
- Sort by any column
- Stats bar: Total SKUs, ATP, FBA, SJIT, FBF
- Inline edit modal (respects column permissions)
- Admin-only delete

### Admin Panel
- Dashboard stats (items, users, logs, imports)
- User CRUD (create, edit, delete, activate/deactivate)
- Per-user column permission matrix (visible + editable per column)
- Audit log viewer with filters
- Import history

### Data Import
- Drag & drop CSV upload
- Auto-detects Uniware Stock CSV format
- Upsert logic: new SKUs inserted, existing SKUs updated
- Import summary (new / updated / skipped)
- Import history tracked

### Audit Logging
- Every login/logout tracked
- Every CRUD operation logged with old + new values
- IP address tracking
- Filterable by action and user

---

## API Endpoints

### Auth
- `POST /api/auth/login` — Login, get JWT token
- `POST /api/auth/logout` — Logout
- `GET  /api/auth/me` — Current user info

### Items
- `GET    /api/items/` — Paginated items (search, filter, sort)
- `GET    /api/items/columns` — User's column permissions
- `GET    /api/items/filters` — Filter options
- `GET    /api/items/{id}` — Single item
- `PUT    /api/items/{id}` — Update item
- `DELETE /api/items/{id}` — Delete item (admin)
- `POST   /api/items/import` — Import CSV (admin)

### Admin
- `GET    /api/admin/users` — List users
- `POST   /api/admin/users` — Create user
- `PUT    /api/admin/users/{id}` — Update user
- `DELETE /api/admin/users/{id}` — Delete user
- `GET    /api/admin/stats` — Dashboard stats
- `GET    /api/admin/import-history` — Import history

### Logs
- `GET /api/logs/` — Paginated audit logs
- `GET /api/logs/actions` — Distinct action types

---

## Performance Notes

- SQLite with WAL mode for concurrent reads
- Indexed queries via LIMIT/OFFSET pagination
- Server-side filtering & sorting (never loads full dataset to client)
- Debounced search (350ms delay)
- Scalable to 500k+ rows with proper indexing

## Adding Indexes for Scale

```sql
CREATE INDEX IF NOT EXISTS idx_sku ON stock_items(sku_code);
CREATE INDEX IF NOT EXISTS idx_status ON stock_items(status);
CREATE INDEX IF NOT EXISTS idx_availability ON stock_items(availability);
CREATE INDEX IF NOT EXISTS idx_category ON stock_items(category);
```
