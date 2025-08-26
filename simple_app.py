#!/usr/bin/env python3
"""
Simple Python Backend for Pension Management System
No external dependencies - uses only built-in Python libraries
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import random
from datetime import datetime, timedelta
import sqlite3
import os
from urllib.parse import urlparse, parse_qs

# Enable CORS
class CORSRequestHandler(BaseHTTPRequestHandler):
    def _set_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
    
    def do_OPTIONS(self):
        self.send_response(200)
        self._set_cors_headers()
        self.end_headers()
    
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self._set_cors_headers()
        self.end_headers()
        
        # Route handling
        if path == '/api/dashboard/stats':
            response = self.get_dashboard_stats()
        elif path == '/api/dashboard/age-distribution':
            response = self.get_age_distribution()
        elif path == '/api/dashboard/state-wise-data':
            response = self.get_state_wise_data()
        elif path == '/api/dashboard/verification-locations':
            response = self.get_verification_locations()
        elif path == '/api/analytics/bar-chart-race-data':
            response = self.get_bar_chart_race_data()
        else:
            response = {'error': 'Not found'}
        
        self.wfile.write(json.dumps(response).encode())
    
    def get_dashboard_stats(self):
        """Get main dashboard statistics"""
        return {
            'totalPensioners': random.randint(45000, 50000),
            'verifiedThisMonth': random.randint(15000, 20000),
            'pendingVerifications': random.randint(4000, 6000),
            'totalAmount': round(random.uniform(1500000000, 2000000000), 2),
            'lastUpdated': datetime.now().isoformat()
        }
    
    def get_age_distribution(self):
        """Get age-wise distribution data"""
        return [
            {'ageGroup': '60-65', 'count': random.randint(8000, 12000)},
            {'ageGroup': '66-70', 'count': random.randint(10000, 15000)},
            {'ageGroup': '71-75', 'count': random.randint(12000, 18000)},
            {'ageGroup': '76-80', 'count': random.randint(8000, 12000)},
            {'ageGroup': '80+', 'count': random.randint(5000, 8000)}
        ]
    
    def get_state_wise_data(self):
        """Get state-wise pension data"""
        states = [
            'Uttar Pradesh', 'Maharashtra', 'Bihar', 'West Bengal', 'Rajasthan',
            'Karnataka', 'Gujarat', 'Andhra Pradesh', 'Tamil Nadu', 'Madhya Pradesh'
        ]
        
        return [{
            'state': state,
            'totalPensioners': random.randint(2000, 8000),
            'verified': random.randint(1500, 6000),
            'pending': random.randint(200, 1000),
            'avgAmount': round(random.uniform(8000, 20000), 2)
        } for state in states]
    
    def get_verification_locations(self):
        """Get verification data for map display"""
        locations_data = [
            {'district': 'Lucknow', 'state': 'Uttar Pradesh', 'coordinates': [26.8467, 80.9462]},
            {'district': 'Mumbai', 'state': 'Maharashtra', 'coordinates': [19.0760, 72.8777]},
            {'district': 'Kolkata', 'state': 'West Bengal', 'coordinates': [22.5726, 88.3639]},
            {'district': 'Chennai', 'state': 'Tamil Nadu', 'coordinates': [13.0827, 80.2707]},
            {'district': 'Bangalore', 'state': 'Karnataka', 'coordinates': [12.9716, 77.5946]},
            {'district': 'Hyderabad', 'state': 'Andhra Pradesh', 'coordinates': [17.3850, 78.4867]},
            {'district': 'Pune', 'state': 'Maharashtra', 'coordinates': [18.5204, 73.8567]},
            {'district': 'Ahmedabad', 'state': 'Gujarat', 'coordinates': [23.0225, 72.5714]},
            {'district': 'Jaipur', 'state': 'Rajasthan', 'coordinates': [26.9124, 75.7873]},
            {'district': 'Surat', 'state': 'Gujarat', 'coordinates': [21.1702, 72.8311]}
        ]
        
        locations = []
        for location in locations_data:
            total = random.randint(50, 500)
            verified = random.randint(30, int(total * 0.8))
            pending = total - verified
            
            locations.append({
                'district': location['district'],
                'state': location['state'],
                'coordinates': location['coordinates'],
                'total': total,
                'verified': verified,
                'pending': pending,
                'status': 'active' if verified > pending else 'pending'
            })
        
        return locations
    
    def get_bar_chart_race_data(self):
        """Get data formatted for bar chart race visualization"""
        states = [
            'Uttar Pradesh', 'Maharashtra', 'Bihar', 'West Bengal', 'Rajasthan',
            'Karnataka', 'Gujarat', 'Andhra Pradesh', 'Tamil Nadu', 'Madhya Pradesh'
        ]
        
        # Create time series data for bar chart race
        months = ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06',
                  '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12']
        
        race_data = {}
        base_values = {state: random.randint(1000, 5000) for state in states}
        
        for i, month in enumerate(months):
            race_data[month] = {}
            for state in states:
                # Simulate growth over time with some randomness
                growth_factor = 1 + (i * 0.15) + random.uniform(-0.1, 0.2)
                race_data[month][state] = int(base_values[state] * growth_factor)
        
        return {
            'data': race_data,
            'title': 'State-wise Pension Verifications Over Time',
            'periods': months
        }

def run_server():
    server_address = ('', 5000)
    httpd = HTTPServer(server_address, CORSRequestHandler)
    
    print("🚀 Pension Management System Backend Started!")
    print("📊 Dashboard API: http://localhost:5000/api/dashboard/stats")
    print("🗺️  Map Data API: http://localhost:5000/api/dashboard/verification-locations")
    print("📈 Bar Chart Race API: http://localhost:5000/api/analytics/bar-chart-race-data")
    print("⚡ Server running on http://localhost:5000")
    print("🔄 Press Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopped")
        httpd.server_close()

if __name__ == '__main__':
    run_server()
