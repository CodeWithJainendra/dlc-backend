import pandas as pd
import os

# Test Excel reading directly
excel_folder = "../XLSx data"
excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]

print(f"Excel files found: {excel_files}")

if excel_files:
    file_path = os.path.join(excel_folder, excel_files[0])
    print(f"Reading: {file_path}")
    
    try:
        df = pd.read_excel(file_path, nrows=5)
        print(f"Columns: {df.columns.tolist()}")
        print("Sample data:")
        print(df)
        
        # Test pincode processing
        for index, row in df.iterrows():
            branch_pin = str(row['BRANCH_PINCODE']) if pd.notna(row['BRANCH_PINCODE']) else ''
            pensioner_pin = str(row['PENSIONER_PINCODE']) if pd.notna(row['PENSIONER_PINCODE']) else ''
            yob = row['YOB'] if pd.notna(row['YOB']) else 0
            
            print(f"Row {index}: Branch={branch_pin}, Pensioner={pensioner_pin}, YOB={yob}")
            
    except Exception as e:
        print(f"Error reading Excel: {e}")
