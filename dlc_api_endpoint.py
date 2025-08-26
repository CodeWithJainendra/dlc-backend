from flask import Flask, jsonify
import json
import os
from collections import defaultdict

app = Flask(__name__)

def load_dlc_analysis_data():
    """Load the latest DLC bank pincode analysis data"""
    try:
        # Find the latest analysis file
        analysis_files = [f for f in os.listdir('.') if f.startswith('dlc_bank_analysis_') and f.endswith('.json')]
        if not analysis_files:
            return None
        
        latest_file = sorted(analysis_files)[-1]
        
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        return data
    except Exception as e:
        print(f"Error loading DLC analysis data: {e}")
        return None

@app.route('/api/dlc-bank-pincode-data', methods=['GET'])
def get_dlc_bank_pincode_data():
    """API endpoint to get DLC completion data by bank pincode"""
    try:
        analysis_data = load_dlc_analysis_data()
        
        if not analysis_data:
            return jsonify({'error': 'No DLC analysis data found'}), 404
        
        # Process data for frontend consumption
        bank_pincode_data = analysis_data.get('bank_pincode_data', {})
        
        # Aggregate by state for state view
        state_wise_data = defaultdict(lambda: {
            'total_dlc_completed': 0,
            'age_groups': defaultdict(int),
            'bank_pincodes': [],
            'pincode_counts': defaultdict(int)
        })
        
        # Aggregate by district for district view
        district_wise_data = defaultdict(lambda: {
            'total_dlc_completed': 0,
            'age_groups': defaultdict(int),
            'bank_pincodes': [],
            'state': ''
        })
        
        for pincode, data in bank_pincode_data.items():
            state = data['state']
            district = get_district_from_pincode(pincode)
            
            # State aggregation
            state_wise_data[state]['total_dlc_completed'] += data['total_dlc_completed']
            state_wise_data[state]['pincode_counts'][pincode] = data['total_dlc_completed']
            state_wise_data[state]['bank_pincodes'].append({
                'pincode': pincode,
                'dlc_count': data['total_dlc_completed'],
                'district': district
            })
            
            for age_group, count in data['age_groups'].items():
                state_wise_data[state]['age_groups'][age_group] += count
            
            # District aggregation
            district_key = f"{district}_{state}"
            district_wise_data[district_key]['total_dlc_completed'] += data['total_dlc_completed']
            district_wise_data[district_key]['state'] = state
            district_wise_data[district_key]['bank_pincodes'].append({
                'pincode': pincode,
                'dlc_count': data['total_dlc_completed']
            })
            
            for age_group, count in data['age_groups'].items():
                district_wise_data[district_key]['age_groups'][age_group] += count
        
        # Convert defaultdicts to regular dicts
        state_final = {}
        for state, data in state_wise_data.items():
            state_final[state] = {
                'total_pensioners': data['total_dlc_completed'],
                'age_groups': dict(data['age_groups']),
                'bank_locations': {},  # Keep for compatibility
                'pincode_counts': dict(data['pincode_counts']),
                'bank_pincodes': sorted(data['bank_pincodes'], key=lambda x: x['dlc_count'], reverse=True)[:20]
            }
        
        district_final = {}
        for district_key, data in district_wise_data.items():
            district_final[district_key] = {
                'total_dlc_completed': data['total_dlc_completed'],
                'age_groups': dict(data['age_groups']),
                'state': data['state'],
                'bank_pincodes': sorted(data['bank_pincodes'], key=lambda x: x['dlc_count'], reverse=True)[:10]
            }
        
        return jsonify({
            'state_wise_data': state_final,
            'district_wise_data': district_final,
            'bank_pincode_data': bank_pincode_data,
            'total_records': analysis_data.get('total_records_processed', 0),
            'total_bank_pincodes': analysis_data.get('total_bank_pincodes', 0),
            'analysis_timestamp': analysis_data.get('analysis_timestamp', '')
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_district_from_pincode(pincode):
    """Get district from pincode (matching frontend logic)"""
    try:
        pin_num = int(str(pincode)[:3])
        
        # Gujarat districts
        if 380 <= pin_num <= 382: return 'Ahmedabad'
        elif 390 <= pin_num <= 396: return 'Vadodara'
        elif 360 <= pin_num <= 370: return 'Rajkot'
        elif 370 <= pin_num <= 375: return 'Jamnagar'
        elif 383 <= pin_num <= 389: return 'Gandhinagar'
        elif 362 <= pin_num <= 365: return 'Bhavnagar'
        
        # Rajasthan districts
        elif 302 <= pin_num <= 303: return 'Jaipur'
        elif 342 <= pin_num <= 344: return 'Jodhpur'
        elif 313 <= pin_num <= 314: return 'Udaipur'
        elif 334 <= pin_num <= 335: return 'Bikaner'
        elif 301 <= pin_num <= 302: return 'Alwar'
        elif 321 <= pin_num <= 322: return 'Bharatpur'
        
        # Maharashtra districts
        elif 400 <= pin_num <= 421: return 'Mumbai'
        elif 411 <= pin_num <= 414: return 'Pune'
        elif 440 <= pin_num <= 445: return 'Nagpur'
        elif 422 <= pin_num <= 425: return 'Nashik'
        elif 431 <= pin_num <= 432: return 'Aurangabad'
        
        # Karnataka districts
        elif 560 <= pin_num <= 562: return 'Bangalore'
        elif 570 <= pin_num <= 571: return 'Mysore'
        elif 580 <= pin_num <= 582: return 'Hubli'
        elif 575 <= pin_num <= 576: return 'Mangalore'
        
        # Bihar districts
        elif 800 <= pin_num <= 803: return 'Patna'
        elif 834 <= pin_num <= 835: return 'Ranchi'
        elif 831 <= pin_num <= 832: return 'Dhanbad'
        
        # Delhi
        elif 110 <= pin_num <= 140: return 'Delhi'
        
        else: return 'Other District'
    except:
        return 'Unknown District'

if __name__ == '__main__':
    app.run(debug=True, port=5001)
