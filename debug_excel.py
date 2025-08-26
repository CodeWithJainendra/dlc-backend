#!/usr/bin/env python3
import pandas as pd
import os
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

# Test Excel processing
excel_folder = "../XLSx data"
excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]

print(f"Found Excel files: {excel_files}")

if excel_files:
    file_path = os.path.join(excel_folder, excel_files[0])
    print(f"Testing file: {file_path}")
    
    # Read first 100 rows to test
    df = pd.read_excel(file_path, nrows=100)
    print(f"Columns: {df.columns.tolist()}")
    print(f"Shape: {df.shape}")
    print("\nFirst 5 rows:")
    print(df.head())
    
    # Test state mapping
    state_data = defaultdict(int)
    
    for _, row in df.iterrows():
        try:
            branch_pincode = str(row['BRANCH_PINCODE']) if pd.notna(row['BRANCH_PINCODE']) else ''
            if branch_pincode:
                bank_state = get_state_from_pincode(branch_pincode)
                state_data[bank_state] += 1
                print(f"Pincode: {branch_pincode} -> State: {bank_state}")
        except Exception as e:
            print(f"Error processing row: {e}")
            continue
    
    print(f"\nState-wise bank verification counts:")
    for state, count in state_data.items():
        print(f"{state}: {count}")
