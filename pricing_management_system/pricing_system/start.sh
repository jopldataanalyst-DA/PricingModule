#!/bin/bash
echo "🚀 Starting Rajnandini Pricing Management System..."
echo ""

# Install dependencies
pip install -r requirements.txt -q

echo ""
echo "✅ Dependencies installed"
echo ""
echo "📊 Starting server on http://localhost:8000"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Default Login Credentials:"
echo "  Admin   → admin / admin123"
echo "  Vikesh  → vikesh / vikesh123"  
echo "  Hitesh  → hitesh / hitesh123"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Open browser: http://localhost:8000"
echo ""

python main.py
