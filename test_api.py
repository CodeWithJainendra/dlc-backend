#!/usr/bin/env python3
import requests
import json

# Test the API endpoint
try:
    response = requests.get('http://localhost:5000/api/dlc-bank-pincode-data')
    data = response.json()
    
    print('API Response Keys:', list(data.keys()))
    print('Has state_wise_data:', 'state_wise_data' in data)
    
    if 'state_wise_data' in data:
        states = list(data['state_wise_data'].keys())
        print(f'States found: {len(states)}')
        print('First 5 states:', states[:5])
        
        if 'Rajasthan' in data['state_wise_data']:
            raj_data = data['state_wise_data']['Rajasthan']
            print(f'Rajasthan total_pensioners: {raj_data.get("total_pensioners", 0):,}')
            print(f'Rajasthan age_groups: {raj_data.get("age_groups", {})}')
        else:
            print('Rajasthan NOT found in state_wise_data')
            print('Available states:', [s for s in states if 'raj' in s.lower() or 'Raj' in s])
    
    # Also check raw bank_pincode_data
    if 'bank_pincode_data' in data:
        raj_pincodes = {k: v for k, v in data['bank_pincode_data'].items() if v.get('state') == 'Rajasthan'}
        print(f'Rajasthan bank pincodes: {len(raj_pincodes)}')
        if raj_pincodes:
            total_raj = sum(v['total_dlc_completed'] for v in raj_pincodes.values())
            print(f'Total Rajasthan DLC from pincodes: {total_raj:,}')
    
except Exception as e:
    print(f'Error: {e}')
