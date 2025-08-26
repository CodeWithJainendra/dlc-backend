#!/usr/bin/env python3
"""
Pension Management System - Python Backend API
Integrates with Vue.js Dashboard for real-time data processing
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import random
from datetime import datetime, timedelta
import sqlite3
import os
from typing import Dict, List, Any
try:
    import pandas as pd  # Optional for local/dev
except Exception:
    pd = None
try:
    import numpy as np  # Optional for local/dev
except Exception:
    np = None
try:
    import bar_chart_race as bcr  # Optional for local/dev
except Exception:
    bcr = None
from collections import defaultdict

app = Flask(__name__)
CORS(app)  # Enable CORS for Vue.js frontend

# Database setup
DB_PATH = os.getenv('DB_PATH', 'pension_data.db')

def init_database():
    """Initialize SQLite database with sample data"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pensioners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pensioner_id TEXT UNIQUE,
            name TEXT,
            age INTEGER,
            district TEXT,
            state TEXT,
            bank TEXT,
            account_number TEXT,
            status TEXT,
            amount REAL,
            last_verification DATE,
            authentication_method TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pension_id TEXT,
            verification_type TEXT,
            status TEXT,
            verified_by TEXT,
            verification_date DATE,
            location TEXT,
            FOREIGN KEY (pension_id) REFERENCES pensioners (pension_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_name TEXT,
            metric_value REAL,
            date DATE,
            category TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def load_excel_data():
    """Load real pensioner data from Excel files with authentication methods"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if data already exists
    try:
        cursor.execute("SELECT COUNT(*) FROM pensioners")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"📊 Database already has {count} records")
            conn.close()
            return
    except sqlite3.OperationalError:
        # Table doesn't exist, will be created by init_database()
        print("📊 Database table doesn't exist, will create and load data")
        pass
    
    if pd is None:
        print("⚠️ Skipping Excel load: pandas not available in this environment")
        conn.close()
        return

    excel_files = [
        '../XLSx data/GAD_DLC_PINCODE_DATA_1.xlsx',
        '../XLSx data/GAD_DLC_PINCODE_DATA_2.xlsx', 
        '../XLSx data/GAD_DLC_PINCODE_DATA_3.xlsx',
        '../XLSx data/GAD_DLC_PINCODE_DATA_4.xlsx',
        '../XLSx data/GAD_DLC_PINCODE_DATA_5.xlsx'
    ]
    
    auth_methods = ['IRIS', 'Fingerprint', 'Face Auth']
    banks = ['SBI', 'HDFC', 'ICICI', 'PNB', 'BOB', 'Canara Bank', 'Union Bank', 'Axis Bank']
    
    # Authentication method distribution by age group
    def get_auth_method_by_age(age):
        if age >= 60 and age <= 65:
            # Younger pensioners prefer Face Auth and Fingerprint
            return random.choices(auth_methods, weights=[25, 40, 35])[0]
        elif age >= 66 and age <= 75:
            # Middle age prefer IRIS and Fingerprint
            return random.choices(auth_methods, weights=[45, 35, 20])[0]
        else:
            # Older pensioners prefer IRIS (more reliable)
            return random.choices(auth_methods, weights=[60, 30, 10])[0]
    
    total_records = 0
    batch_size = 1000
    global_counter = 1  # Global counter across all files
    
    print("📂 Loading Excel files with authentication methods...")
    
    for file_path in excel_files:
        if not os.path.exists(file_path):
            print(f"⚠️ File not found: {file_path}")
            continue
            
        try:
            print(f"📖 Reading {file_path}...")
            df = pd.read_excel(file_path)
            
            # If columns don't match, try common variations
            actual_columns = df.columns.tolist()
            print(f"📋 Available columns: {actual_columns[:10]}...")  # Show first 10 columns
            
            # Process data in batches
            batch_data = []
            
            for index, row in df.iterrows():
                try:
                    # Extract data based on available columns
                    pensioner_id = f"DLC{global_counter:08d}"
                    global_counter += 1
                    name = str(row.iloc[1]) if len(row) > 1 else f"Pensioner {index}"
                    age = random.randint(60, 85)  # Generate age if not available
                    district = str(row.iloc[3]) if len(row) > 3 else "Unknown"
                    state = str(row.iloc[2]) if len(row) > 2 else "Unknown" 
                    bank = random.choice(banks)
                    account_number = f"{random.randint(100000, 999999)}"
                    status = random.choice(['Verified', 'Pending', 'Under Review'])
                    amount = float(row.iloc[4]) if len(row) > 4 and pd.notna(row.iloc[4]) else random.uniform(5000, 25000)
                    last_verification = (datetime.now() - timedelta(days=random.randint(1, 365))).date()
                    auth_method = get_auth_method_by_age(age)
                    
                    batch_data.append((
                        pensioner_id, name, age, district, state, bank, account_number, status, amount, last_verification, auth_method
                    ))
                    
                    # Insert in batches
                    if len(batch_data) >= batch_size:
                        cursor.executemany('''
                            INSERT INTO pensioners (pensioner_id, name, age, district, state, bank, account_number, status, amount, last_verification, authentication_method)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', batch_data)
                        conn.commit()
                        total_records += len(batch_data)
                        print(f"✅ Inserted {total_records} records...")
                        batch_data = []
                        
                except Exception as e:
                    print(f"⚠️ Error processing row {index}: {e}")
                    continue
            
            # Insert remaining batch
            if batch_data:
                cursor.executemany('''
                    INSERT INTO pensioners (pensioner_id, name, age, district, state, bank, account_number, status, amount, last_verification, authentication_method)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch_data)
                conn.commit()
                total_records += len(batch_data)
                
            print(f"✅ Completed {file_path}: {len(df)} rows processed")
            
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            continue
    
    print(f"🎉 Total records loaded from Excel: {total_records}")
    conn.close()

# API Routes

@app.route('/api/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    """Get main dashboard statistics"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Total registered pensioners
    cursor.execute("SELECT COUNT(*) FROM pensioners")
    total_pensioners = cursor.fetchone()[0]
    
    # Verified this month
    cursor.execute("""
        SELECT COUNT(*) FROM pensioners 
        WHERE status = 'Verified' AND last_verification >= date('now', '-30 days')
    """)
    verified_this_month = cursor.fetchone()[0]
    
    # Pending verifications
    cursor.execute("SELECT COUNT(*) FROM pensioners WHERE status = 'Pending'")
    pending_verifications = cursor.fetchone()[0]
    
    # Total amount disbursed
    cursor.execute("SELECT SUM(amount) FROM pensioners WHERE status = 'Verified'")
    total_amount = cursor.fetchone()[0] or 0
    
    conn.close()
    
    return jsonify({
        'totalPensioners': total_pensioners,
        'verifiedThisMonth': verified_this_month,
        'pendingVerifications': pending_verifications,
        'totalAmount': round(total_amount, 2),
        'lastUpdated': datetime.now().isoformat()
    })

@app.route('/api/dashboard/age-distribution', methods=['GET'])
def get_age_distribution():
    """Get age-wise distribution data"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            CASE 
                WHEN age BETWEEN 60 AND 65 THEN '60-65'
                WHEN age BETWEEN 66 AND 70 THEN '66-70'
                WHEN age BETWEEN 71 AND 75 THEN '71-75'
                WHEN age BETWEEN 76 AND 80 THEN '76-80'
                ELSE '80+'
            END as age_group,
            COUNT(*) as count
        FROM pensioners 
        GROUP BY age_group
        ORDER BY age_group
    """)
    
    results = cursor.fetchall()
    conn.close()
    
    return jsonify([{'ageGroup': row[0], 'count': row[1]} for row in results])

@app.route('/api/dashboard/state-wise-data', methods=['GET'])
def get_state_wise_data():
    """Get state-wise pension data"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            state,
            COUNT(*) as total_pensioners,
            SUM(CASE WHEN status = 'Verified' THEN 1 ELSE 0 END) as verified,
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as pending,
            ROUND(AVG(amount), 2) as avg_amount
        FROM pensioners 
        GROUP BY state
        ORDER BY total_pensioners DESC
    """)
    
    results = cursor.fetchall()
    conn.close()
    
    return jsonify([{
        'state': row[0],
        'totalPensioners': row[1],
        'verified': row[2],
        'pending': row[3],
        'avgAmount': row[4]
    } for row in results])

@app.route('/api/dashboard/authentication-methods', methods=['GET'])
def get_authentication_methods():
    """Get authentication method distribution with age group filtering"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get age group filter from query params
    age_group = request.args.get('age_group', None)
    
    # Base query for authentication methods
    base_query = """
        SELECT 
            authentication_method,
            COUNT(*) as count,
            CASE 
                WHEN age BETWEEN 60 AND 65 THEN '60-65'
                WHEN age BETWEEN 66 AND 70 THEN '66-70'
                WHEN age BETWEEN 71 AND 75 THEN '71-75'
                WHEN age BETWEEN 76 AND 80 THEN '76-80'
                ELSE '80+'
            END as age_group
        FROM pensioners 
        WHERE authentication_method IS NOT NULL
    """
    
    # Add age group filter if specified
    if age_group:
        if age_group == '60-65':
            base_query += " AND age BETWEEN 60 AND 65"
        elif age_group == '66-70':
            base_query += " AND age BETWEEN 66 AND 70"
        elif age_group == '71-75':
            base_query += " AND age BETWEEN 71 AND 75"
        elif age_group == '76-80':
            base_query += " AND age BETWEEN 76 AND 80"
        elif age_group == '80+':
            base_query += " AND age > 80"
    
    base_query += " GROUP BY authentication_method, age_group ORDER BY authentication_method"
    
    cursor.execute(base_query)
    results = cursor.fetchall()
    
    # Process results into structured format
    auth_data = {}
    age_breakdown = {}
    
    for row in results:
        auth_method, count, age_grp = row
        if auth_method not in auth_data:
            auth_data[auth_method] = 0
            age_breakdown[auth_method] = {}
        
        auth_data[auth_method] += count
        age_breakdown[auth_method][age_grp] = count
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM pensioners WHERE authentication_method IS NOT NULL")
    total_count = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        'authenticationMethods': auth_data,
        'ageBreakdown': age_breakdown,
        'totalCount': total_count,
        'filteredBy': age_group
    })

@app.route('/api/dashboard/verification-locations', methods=['GET'])
def get_verification_locations():
    """Get verification data for map display"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get district-wise verification data with coordinates (mock coordinates for demo)
    cursor.execute("""
        SELECT 
            district,
            state,
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Verified' THEN 1 ELSE 0 END) as verified,
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as pending
        FROM pensioners 
        GROUP BY district, state
        HAVING total > 5
        ORDER BY total DESC
        LIMIT 50
    """)
    
    results = cursor.fetchall()
    conn.close()
    
    # Mock coordinates for Indian districts (in real app, use proper geocoding)
    mock_coordinates = {
        'Lucknow': [26.8467, 80.9462],
        'Mumbai': [19.0760, 72.8777],
        'Kolkata': [22.5726, 88.3639],
        'Chennai': [13.0827, 80.2707],
        'Bangalore': [12.9716, 77.5946],
        'Hyderabad': [17.3850, 78.4867],
        'Pune': [18.5204, 73.8567],
        'Ahmedabad': [23.0225, 72.5714],
        'Jaipur': [26.9124, 75.7873],
        'Surat': [21.1702, 72.8311]
    }
    
    locations = []
    for row in results:
        district, state, total, verified, pending = row
        coords = mock_coordinates.get(district, [
            20 + random.uniform(-8, 15),  # Random lat between 12-35
            77 + random.uniform(-10, 15)  # Random lng between 67-92
        ])
        
        locations.append({
            'district': district,
            'state': state,
            'coordinates': coords,
            'total': total,
            'verified': verified,
            'pending': pending,
            'status': 'active' if verified > pending else 'pending'
        })
    
    return jsonify(locations)

@app.route('/api/pensioners', methods=['GET'])
def get_pensioners():
    """Get paginated list of pensioners"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    status_filter = request.args.get('status', '')
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = "SELECT * FROM pensioners"
    params = []
    
    if status_filter:
        query += " WHERE status = ?"
        params.append(status_filter)
    
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Get column names
    columns = [description[0] for description in cursor.description]
    
    # Convert to list of dictionaries
    pensioners = [dict(zip(columns, row)) for row in results]
    
    conn.close()
    
    return jsonify({
        'data': pensioners,
        'page': page,
        'per_page': per_page,
        'total': len(pensioners)
    })

@app.route('/pensioners', methods=['GET'])
def get_excel_pensioners():
    try:
        if pd is None:
            return jsonify({'error': 'pandas not installed; Excel processing disabled in this environment', 'pensioners': [], 'total': 0, 'state_summary': {}}), 503
        excel_folder = "../XLSx data"
        pensioners = []
        state_summary = {}
        
        # Get all Excel files
        excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
        print(f"Found {len(excel_files)} Excel files")
        
        # Process ALL 5 files
        for file_index, excel_file in enumerate(excel_files):
            file_path = os.path.join(excel_folder, excel_file)
            print(f"Processing file {file_index + 1}/{len(excel_files)}: {excel_file}")
            
            try:
                # Read Excel file (process more rows for comprehensive data)
                df = pd.read_excel(file_path, nrows=2000)
                print(f"File shape: {df.shape}")
                
                for index, row in df.iterrows():
                    try:
                        # Extract data from row
                        pensioner_pincode = str(row['PENSIONER_PINCODE']) if pd.notna(row['PENSIONER_PINCODE']) else ''
                        branch_pincode = str(row['BRANCH_PINCODE']) if pd.notna(row['BRANCH_PINCODE']) else ''
                        
                        # Clean pincode data
                        if pensioner_pincode.endswith('.0'):
                            pensioner_pincode = pensioner_pincode[:-2]
                        if branch_pincode.endswith('.0'):
                            branch_pincode = branch_pincode[:-2]
                        
                        # Skip if no valid pincode
                        if not pensioner_pincode or pensioner_pincode == 'nan':
                            continue
                            
                        # Get state and district from pincodes
                        pensioner_state = get_state_from_pincode(pensioner_pincode)
                        branch_state = get_state_from_pincode(branch_pincode)
                        pensioner_district = get_district_from_pincode(pensioner_pincode)
                        branch_district = get_district_from_pincode(branch_pincode)
                        
                        # Skip unknown states
                        if pensioner_state == 'Unknown' or pensioner_state == 'Other States':
                            continue
                        
                        # Initialize state summary if not exists
                        if pensioner_state not in state_summary:
                            state_summary[pensioner_state] = {
                                'total_pensioners': 0,
                                'districts': set(),
                                'pincodes': set(),
                                'banks': set()
                            }
                        
                        # Update state summary
                        state_summary[pensioner_state]['total_pensioners'] += 1
                        state_summary[pensioner_state]['districts'].add(pensioner_district)
                        state_summary[pensioner_state]['pincodes'].add(pensioner_pincode)
                        if pd.notna(row.get('BANK_NAME')):
                            state_summary[pensioner_state]['banks'].add(str(row['BANK_NAME']))
                        
                        # Create pensioner record
                        pensioner = {
                            'id': f"{file_index}_{index}",
                            'name': f"Pensioner {len(pensioners) + 1}",
                            'pensioner_pincode': pensioner_pincode,
                            'branch_pincode': branch_pincode,
                            'pensioner_state': pensioner_state,
                            'branch_state': branch_state,
                            'pensioner_district': pensioner_district,
                            'branch_district': branch_district,
                            'bank': str(row.get('BANK_NAME', 'Unknown Bank')),
                            'amount': float(row.get('PENSION_AMOUNT', 0)) if pd.notna(row.get('PENSION_AMOUNT')) else 0,
                            'verification_date': datetime.now().strftime('%Y-%m-%d')
                        }
                        
                        pensioners.append(pensioner)
                        
                    except Exception as e:
                        print(f"Error processing row {index} in file {excel_file}: {e}")
                        continue
                        
            except Exception as e:
                print(f"Error reading file {excel_file}: {e}")
                continue
        
        # Convert sets to counts for JSON serialization
        final_state_summary = {}
        for state, data in state_summary.items():
            final_state_summary[state] = {
                'total_pensioners': data['total_pensioners'],
                'total_districts': len(data['districts']),
                'total_pincodes': len(data['pincodes']),
                'total_banks': len(data['banks']),
                'districts': list(data['districts']),
                'pincodes': list(data['pincodes']),
                'banks': list(data['banks'])
            }
        
        print(f"Processed {len(pensioners)} pensioner records from {len(excel_files)} files")
        print(f"States found: {list(final_state_summary.keys())}")
        
        return jsonify({
            'pensioners': pensioners,
            'total': len(pensioners),
            'processed_files': len(excel_files),
            'state_summary': final_state_summary
        })
        
    except Exception as e:
        print(f"Error in get_pensioners: {e}")
        return jsonify({
            'error': str(e),
            'pensioners': [],
            'total': 0,
            'state_summary': {}
        }), 500

@app.route('/api/analytics/trends', methods=['GET'])
def get_analytics_trends():
    """Get analytics trends data"""
    days = request.args.get('days', 30, type=int)

    # Generate mock trend data
    dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days, 0, -1)]

    trends = {
        'verifications': [random.randint(50, 200) for _ in dates],
        'registrations': [random.randint(20, 100) for _ in dates],
        'disbursements': [random.randint(100000, 500000) for _ in dates],
        'dates': dates
    }

    return jsonify(trends)

@app.route('/api/analytics/bar-chart-race-data', methods=['GET'])
def get_bar_chart_race_data():
    """Get data formatted for bar chart race visualization"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get state-wise data over time (simulated monthly data)
    cursor.execute("""
        SELECT
            state,
            COUNT(*) as total_pensioners,
            SUM(CASE WHEN status = 'Verified' THEN 1 ELSE 0 END) as verified
        FROM pensioners
        GROUP BY state
        ORDER BY total_pensioners DESC
        LIMIT 10
    """)

    results = cursor.fetchall()
    conn.close()

    # Create time series data for bar chart race
    # Simulate 12 months of data
    months = ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06',
              '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12']

    race_data = {}
    for month in months:
        race_data[month] = {}
        for state, total, verified in results:
            # Simulate growth over time
            growth_factor = 1 + (months.index(month) * 0.1)
            race_data[month][state] = int(verified * growth_factor * random.uniform(0.8, 1.2))

    return jsonify({
        'data': race_data,
        'title': 'State-wise Pension Verifications Over Time',
        'periods': months
    })

def get_state_from_pincode(pincode):
    """Get state from pincode using first 3 digits"""
    try:
        pin_num = int(str(pincode)[:3])
        
        if 110 <= pin_num <= 140:
            return 'Delhi'
        elif 121 <= pin_num <= 136:
            return 'Haryana'
        elif 140 <= pin_num <= 160:
            return 'Punjab'
        elif 301 <= pin_num <= 345:
            return 'Rajasthan'
        elif 201 <= pin_num <= 285:
            return 'Uttar Pradesh'
        elif 800 <= pin_num <= 855:
            return 'Bihar'
        elif 700 <= pin_num <= 743:
            return 'West Bengal'
        elif 400 <= pin_num <= 445:
            return 'Maharashtra'
        elif 380 <= pin_num <= 396:
            return 'Gujarat'
        elif 560 <= pin_num <= 591:
            return 'Karnataka'
        elif 600 <= pin_num <= 643:
            return 'Tamil Nadu'
        elif 500 <= pin_num <= 509:
            return 'Telangana'
        elif 515 <= pin_num <= 535:
            return 'Andhra Pradesh'
        elif 450 <= pin_num <= 492:
            return 'Madhya Pradesh'
        elif 751 <= pin_num <= 770:
            return 'Odisha'
        elif 781 <= pin_num <= 788:
            return 'Assam'
        elif 682 <= pin_num <= 695:
            return 'Kerala'
        elif 831 <= pin_num <= 835:
            return 'Jharkhand'
        elif 248 <= pin_num <= 263:
            return 'Uttarakhand'
        elif 171 <= pin_num <= 177:
            return 'Himachal Pradesh'
        else:
            return 'Other States'
    except:
        return 'Unknown'

def get_district_from_pincode(pincode):
    """Get district from pincode using simplified mapping"""
    try:
        pin_num = int(str(pincode)[:3])
        
        # Gujarat districts
        if 360 <= pin_num <= 370:
            return 'Rajkot'
        elif 380 <= pin_num <= 382:
            return 'Ahmedabad'
        elif 390 <= pin_num <= 396:
            return 'Vadodara'
        elif 360 <= pin_num <= 365:
            return 'Rajkot'
        elif 370 <= pin_num <= 375:
            return 'Jamnagar'
        elif 383 <= pin_num <= 389:
            return 'Gandhinagar'
        
        # Maharashtra districts
        elif 400 <= pin_num <= 421:
            return 'Mumbai'
        elif 411 <= pin_num <= 414:
            return 'Pune'
        elif 440 <= pin_num <= 445:
            return 'Nagpur'
        elif 422 <= pin_num <= 425:
            return 'Nashik'
        
        # Karnataka districts
        elif 560 <= pin_num <= 562:
            return 'Bangalore'
        elif 570 <= pin_num <= 571:
            return 'Mysore'
        elif 580 <= pin_num <= 582:
            return 'Hubli'
        elif 575 <= pin_num <= 576:
            return 'Mangalore'
        
        # Tamil Nadu districts
        elif 600 <= pin_num <= 603:
            return 'Chennai'
        elif 641 <= pin_num <= 642:
            return 'Coimbatore'
        elif 625 <= pin_num <= 626:
            return 'Madurai'
        elif 620 <= pin_num <= 621:
            return 'Tiruchirappalli'
        
        # Uttar Pradesh districts
        elif 226 <= pin_num <= 227:
            return 'Lucknow'
        elif 208 <= pin_num <= 209:
            return 'Kanpur'
        elif 282 <= pin_num <= 283:
            return 'Agra'
        elif 221 <= pin_num <= 222:
            return 'Varanasi'
        
        # West Bengal districts
        elif 700 <= pin_num <= 711:
            return 'Kolkata'
        elif 711 <= pin_num <= 712:
            return 'Howrah'
        elif 713 <= pin_num <= 714:
            return 'Hooghly'
        
        # Rajasthan districts
        elif 302 <= pin_num <= 303:
            return 'Jaipur'
        elif 342 <= pin_num <= 344:
            return 'Jodhpur'
        elif 324 <= pin_num <= 325:
            return 'Kota'
        elif 334 <= pin_num <= 335:
            return 'Bikaner'
        
        # Bihar districts
        elif 800 <= pin_num <= 801:
            return 'Patna'
        elif 823 <= pin_num <= 824:
            return 'Gaya'
        elif 812 <= pin_num <= 813:
            return 'Bhagalpur'
        elif 842 <= pin_num <= 843:
            return 'Muzaffarpur'
        
        else:
            return 'Other District'
    except:
        return 'Unknown District'

def get_age_group(birth_year):
    """Get age group from birth year"""
    try:
        current_year = datetime.now().year
        age = current_year - int(birth_year)
        
        if age < 60:
            return 'Below 60'
        elif 60 <= age <= 65:
            return '60-65'
        elif 66 <= age <= 70:
            return '66-70'
        elif 71 <= age <= 75:
            return '71-75'
        elif 76 <= age <= 80:
            return '76-80'
        else:
            return '80+'
    except:
        return 'Unknown'

@app.route('/api/dlc-bank-pincode-data', methods=['GET'])
def get_dlc_bank_pincode_data():
    """API endpoint to get DLC completion data by bank pincode from analysis files"""
    try:
        # Find the latest analysis file (should be dlc_bank_analysis_20250825_120946.json)
        analysis_files = [f for f in os.listdir('.') if f.startswith('dlc_bank_analysis_') and f.endswith('.json')]
        if not analysis_files:
            return jsonify({'error': 'No DLC analysis data found'}), 404
        
        latest_file = sorted(analysis_files)[-1]
        print(f"📊 Loading DLC analysis from: {latest_file}")
        
        with open(latest_file, 'r') as f:
            analysis_data = json.load(f)
        
        # Process data for frontend consumption
        bank_pincode_data = analysis_data.get('bank_pincode_data', {})
        
        # Aggregate by pensioner residence state (where pensioners live)
        state_wise_data = defaultdict(lambda: {
            'total_pensioners': 0,
            'age_groups': defaultdict(int),
            'bank_locations': defaultdict(int),
            'pincode_counts': defaultdict(int),
            'pensioner_pincodes': defaultdict(int)  # Track where pensioners actually live
        })
        
        print(f"🏦 Processing {len(bank_pincode_data)} bank pincodes...")
        
        # Process each bank pincode and distribute pensioners to their residence states
        for bank_pincode, data in bank_pincode_data.items():
            bank_state = data['state']  # Where the bank is located
            
            # Distribute pensioners to their residence states
            for pensioner_state, pensioner_count in data.get('pensioner_states', {}).items():
                if pensioner_state and pensioner_state != 'Invalid Pincode' and pensioner_state != 'Other State':
                    # Add pensioners to their residence state
                    state_wise_data[pensioner_state]['total_pensioners'] += pensioner_count
                    
                    # Track which bank processed their DLC (for reference)
                    state_wise_data[pensioner_state]['bank_locations'][bank_state] += pensioner_count
                    
                    # Distribute age groups proportionally
                    total_bank_dlc = data['total_dlc_completed']
                    if total_bank_dlc > 0:
                        for age_group, age_count in data['age_groups'].items():
                            proportional_count = int((age_count * pensioner_count) / total_bank_dlc)
                            state_wise_data[pensioner_state]['age_groups'][age_group] += proportional_count
        
        # Convert defaultdicts to regular dicts
        state_final = {}
        for state, data in state_wise_data.items():
            state_final[state] = {
                'total_pensioners': data['total_pensioners'],
                'age_groups': dict(data['age_groups']),
                'bank_locations': dict(data['bank_locations']),
                'pincode_counts': dict(data['pincode_counts'])
            }
        
        # Log Rajasthan data for debugging
        raj_data = state_final.get('Rajasthan', {})
        print(f"🎯 Rajasthan DLC Total: {raj_data.get('total_pensioners', 0):,}")
        print(f"🏦 Rajasthan Bank Pincodes: {len(raj_data.get('pincode_counts', {}))}")
        
        return jsonify({
            'state_wise_data': state_final,
            'bank_pincode_data': bank_pincode_data,  
            'total_records': len(bank_pincode_data),
            'total_states': len(state_final),
            'processed_at': analysis_data.get('analysis_timestamp', 'Unknown')
        })
        
    except Exception as e:
        print(f"❌ API Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel-pensioner-data', methods=['GET'])
def get_excel_pensioner_data():
    """DEPRECATED: Use /api/dlc-bank-pincode-data instead - this endpoint now redirects to use analysis data"""
    print("⚠️  Deprecated Excel API called - redirecting to DLC analysis data")
    
    # Redirect to use the pre-analyzed data instead of processing Excel files
    return get_dlc_bank_pincode_data()


@app.route('/api/excel-age-group-summary', methods=['GET'])
def get_excel_age_group_summary():
    """Get age group summary from Excel data"""
    try:
        if pd is None:
            return jsonify({'error': 'pandas not installed; Excel processing disabled in this environment'}), 503
        excel_folder = "../XLSx data"
        age_groups = defaultdict(int)
        
        excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
        
        if excel_files:
            file_path = os.path.join(excel_folder, excel_files[0])
            df = pd.read_excel(file_path, nrows=50000)
            
            for _, row in df.iterrows():
                try:
                    birth_year = int(row['YOB']) if pd.notna(row['YOB']) else 1960
                    age_group = get_age_group(birth_year)
                    age_groups[age_group] += 1
                except:
                    continue
        
        return jsonify([
            {'ageGroup': group, 'count': count} 
            for group, count in age_groups.items()
        ])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def generate_sample_data():
    """Generate sample pensioner data with authentication methods"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if data already exists
    cursor.execute("SELECT COUNT(*) FROM pensioners")
    if cursor.fetchone()[0] > 0:
        conn.close()
        return
    
    # Sample data generation
    states = ['Karnataka', 'Maharashtra', 'Tamil Nadu', 'Gujarat', 'Rajasthan', 'West Bengal', 'Uttar Pradesh', 'Kerala']
    districts = {
        'Karnataka': ['Bangalore', 'Mysore', 'Hubli', 'Mangalore'],
        'Maharashtra': ['Mumbai', 'Pune', 'Nagpur', 'Nashik'],
        'Tamil Nadu': ['Chennai', 'Coimbatore', 'Madurai', 'Salem'],
        'Gujarat': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot'],
        'Rajasthan': ['Jaipur', 'Jodhpur', 'Udaipur', 'Kota'],
        'West Bengal': ['Kolkata', 'Howrah', 'Durgapur', 'Asansol'],
        'Uttar Pradesh': ['Lucknow', 'Kanpur', 'Agra', 'Varanasi'],
        'Kerala': ['Thiruvananthapuram', 'Kochi', 'Kozhikode', 'Thrissur']
    }
    
    banks = ['SBI', 'HDFC', 'ICICI', 'PNB', 'BOB', 'Canara Bank', 'Union Bank', 'Axis Bank']
    statuses = ['Verified', 'Pending', 'Rejected']
    auth_methods = ['IRIS', 'Fingerprint', 'Face Auth']
    
    # Authentication method distribution by age group
    def get_auth_method_by_age(age):
        if age >= 60 and age <= 65:
            # Younger pensioners prefer Face Auth and Fingerprint
            return random.choices(auth_methods, weights=[25, 40, 35])[0]
        elif age >= 66 and age <= 75:
            # Middle age prefer IRIS and Fingerprint
            return random.choices(auth_methods, weights=[45, 35, 20])[0]
        else:
            # Older pensioners prefer IRIS (more reliable)
            return random.choices(auth_methods, weights=[60, 30, 10])[0]
    
    for i in range(1000):
        state = random.choice(states)
        district = random.choice(districts[state])
        age = random.randint(60, 85)
        auth_method = get_auth_method_by_age(age)
        
        pensioner_data = (
            f"P{i+1:06d}",  # pensioner_id
            f"Pensioner {i+1}",  # name
            age,  # age
            state,
            district,
            random.choice(banks),  # bank
            f"{random.randint(100000, 999999)}",  # account_number
            random.choice(statuses),  # status
            round(random.uniform(5000, 25000), 2),  # amount
            (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),  # last_verification
            auth_method  # authentication_method
        )
        
        cursor.execute("""
            INSERT INTO pensioners 
            (pensioner_id, name, age, state, district, bank, account_number, status, amount, last_verification, authentication_method)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, pensioner_data)
    
    conn.commit()
    conn.close()
    print("Sample data with authentication methods generated successfully!")

def migrate_database():
    """Add authentication_method column to existing database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Drop and recreate table with proper schema
        print("🔧 Recreating database with authentication_method column...")
        cursor.execute("DROP TABLE IF EXISTS pensioners")
        cursor.execute('''
            CREATE TABLE pensioners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pensioner_id TEXT UNIQUE,
                name TEXT,
                age INTEGER,
                district TEXT,
                state TEXT,
                bank TEXT,
                account_number TEXT,
                status TEXT,
                amount REAL,
                last_verification DATE,
                authentication_method TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        print("✅ Database recreated with authentication_method column!")
            
    except Exception as e:
        print(f"❌ Migration error: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    init_database()
    if os.environ.get('RUN_MIGRATIONS', 'false').lower() == 'true':
        migrate_database()
    if os.environ.get('LOAD_EXCEL', 'false').lower() == 'true':
        load_excel_data()  # Use real Excel data if explicitly enabled
    
    print("🚀 Pension Management System Backend Started!")
    print("📊 Dashboard API: http://localhost:5000/api/dashboard/stats")
    print("🔐 Authentication API: http://localhost:5000/api/dashboard/authentication-methods")
    print("👥 Pensioners API: http://localhost:5000/api/pensioners")
    print("📂 Using real Excel data with authentication methods")
    
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
