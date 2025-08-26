# 🐍 Python Backend for Pension Management System

## Overview

Python Flask backend that provides real-time data APIs for the Vue.js dashboard.

## Features

- 📊 **Real-time Analytics**: Dashboard statistics and trends
- 🗺️ **Map Integration**: Verification locations with coordinates
- 👥 **Pensioner Management**: CRUD operations for pensioner data
- 📈 **Data Visualization**: Age distribution, state-wise analytics
- 🔄 **Auto-sync**: Real-time data updates for frontend

## Quick Start

### 1. Install Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Run the Server

```bash
python app.py
```

The server will start on `http://localhost:5000`

### 3. API Endpoints

#### Dashboard APIs

- `GET /api/dashboard/stats` - Main dashboard statistics
- `GET /api/dashboard/age-distribution` - Age-wise distribution
- `GET /api/dashboard/state-wise-data` - State-wise pension data
- `GET /api/dashboard/verification-locations` - Map locations

#### Data APIs

- `GET /api/pensioners` - Paginated pensioner list
- `GET /api/analytics/trends` - Analytics trends data

### 4. Database

- Uses SQLite database (`pension_data.db`)
- Auto-generates 1000 sample records on first run
- Includes pensioners, verifications, and analytics tables

## Integration with Vue.js

The Python backend automatically integrates with your Vue.js dashboard:

1. **Auto-detection**: Frontend checks backend availability
2. **Fallback**: Uses mock data if backend unavailable
3. **Real-time**: Live data updates from Python APIs

## Sample Data

The backend generates realistic Indian pension data:

- **States**: All major Indian states
- **Districts**: Real district names
- **Names**: Hindi/Indian names
- **Amounts**: Realistic pension amounts (₹5,000 - ₹25,000)

## Development

### Adding New APIs

1. Add route in `app.py`
2. Update TypeScript types in `src/services/pythonApi.ts`
3. Use in Vue components

### Database Schema

```sql
-- Pensioners table
CREATE TABLE pensioners (
    id INTEGER PRIMARY KEY,
    pension_id TEXT UNIQUE,
    name TEXT,
    age INTEGER,
    district TEXT,
    state TEXT,
    status TEXT,
    amount REAL,
    last_verification DATE
);

-- Verifications table
CREATE TABLE verifications (
    id INTEGER PRIMARY KEY,
    pension_id TEXT,
    verification_type TEXT,
    status TEXT,
    verified_by TEXT,
    verification_date DATE,
    location TEXT
);
```

## Production Deployment

For production, consider:

- Use PostgreSQL instead of SQLite
- Add authentication/authorization
- Implement rate limiting
- Add logging and monitoring
- Use Gunicorn/uWSGI for serving

## Troubleshooting

### Backend Not Starting

```bash
# Check Python version (3.7+)
python --version

# Install dependencies
pip install -r requirements.txt

# Run with debug
python app.py
```

### CORS Issues

The backend includes CORS headers for development. For production, configure proper CORS settings.

### Database Issues

Delete `pension_data.db` to regenerate sample data:

```bash
rm pension_data.db
python app.py
```
