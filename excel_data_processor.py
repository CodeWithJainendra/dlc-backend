#!/usr/bin/env python3
"""
Simplified Excel Data Processor for Pensioner Data
Processes GAD_DLC_PINCODE_DATA Excel files efficiently
"""

import pandas as pd
import os
import json
from datetime import datetime
from collections import defaultdict

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

def process_excel_data():
    """Process Excel files and create state-wise analysis"""
    excel_folder = "../XLSx data"
    
    # Initialize data structures
    state_data = defaultdict(lambda: {
        'total_pensioners': 0,
        'age_groups': defaultdict(int),
        'bank_locations': defaultdict(int)
    })
    
    excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
    print(f"Processing {len(excel_files)} Excel files...")
    
    total_records = 0
    
    for file_name in excel_files:
        file_path = os.path.join(excel_folder, file_name)
        print(f"Processing {file_name}...")
        
        try:
            # Read Excel file in chunks to handle large files
            chunk_size = 10000
            for chunk in pd.read_excel(file_path, chunksize=chunk_size):
                for _, row in chunk.iterrows():
                    try:
                        pensioner_pincode = str(row['PENSIONER_PINCODE']) if pd.notna(row['PENSIONER_PINCODE']) else ''
                        branch_pincode = str(row['BRANCH_PINCODE']) if pd.notna(row['BRANCH_PINCODE']) else ''
                        birth_year = int(row['YOB']) if pd.notna(row['YOB']) else 1960
                        
                        # Get state from pensioner pincode
                        state = get_state_from_pincode(pensioner_pincode)
                        age_group = get_age_group(birth_year)
                        bank_state = get_state_from_pincode(branch_pincode)
                        
                        # Update state data
                        state_data[state]['total_pensioners'] += 1
                        state_data[state]['age_groups'][age_group] += 1
                        state_data[state]['bank_locations'][bank_state] += 1
                        
                        total_records += 1
                        
                        if total_records % 100000 == 0:
                            print(f"Processed {total_records} records...")
                            
                    except Exception as e:
                        continue
                        
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")
            continue
    
    # Convert defaultdict to regular dict for JSON serialization
    result = {}
    for state, data in state_data.items():
        result[state] = {
            'total_pensioners': data['total_pensioners'],
            'age_groups': dict(data['age_groups']),
            'bank_locations': dict(data['bank_locations'])
        }
    
    # Create summary data
    summary = {
        'state_wise_data': result,
        'total_records': total_records,
        'total_states': len(result),
        'processed_at': datetime.now().isoformat()
    }
    
    # Save to JSON
    with open('pensioner_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\n=== Processing Complete ===")
    print(f"Total records: {total_records}")
    print(f"States found: {len(result)}")
    print("Data saved to pensioner_analysis.json")
    
    return summary

if __name__ == "__main__":
    process_excel_data()
