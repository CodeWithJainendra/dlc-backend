import pandas as pd
import os
from collections import defaultdict

def get_state_from_pincode(pincode):
    try:
        pin_num = int(str(pincode)[:3])
        
        if 110 <= pin_num <= 140:
            return 'Delhi'
        elif 201 <= pin_num <= 285:
            return 'Uttar Pradesh'
        elif 400 <= pin_num <= 445:
            return 'Maharashtra'
        elif 560 <= pin_num <= 591:
            return 'Karnataka'
        elif 600 <= pin_num <= 643:
            return 'Tamil Nadu'
        elif 700 <= pin_num <= 743:
            return 'West Bengal'
        elif 800 <= pin_num <= 855:
            return 'Bihar'
        else:
            return 'Other States'
    except:
        return 'Unknown'

# Direct test
excel_folder = "../XLSx data"
file_path = os.path.join(excel_folder, "GAD_DLC_PINCODE_DATA_1.xlsx")

print("Testing Excel processing...")
df = pd.read_excel(file_path, nrows=100)
print(f"Loaded {len(df)} rows")
print(f"Columns: {df.columns.tolist()}")

state_counts = defaultdict(int)

for index, row in df.iterrows():
    branch_pin = str(row['BRANCH_PINCODE']) if pd.notna(row['BRANCH_PINCODE']) else ''
    
    if branch_pin.endswith('.0'):
        branch_pin = branch_pin[:-2]
    
    if branch_pin and len(branch_pin) >= 3:
        state = get_state_from_pincode(branch_pin)
        state_counts[state] += 1
        
        if index < 10:
            print(f"Row {index}: PIN {branch_pin} -> {state}")

print("\nState counts:")
for state, count in state_counts.items():
    print(f"{state}: {count}")
