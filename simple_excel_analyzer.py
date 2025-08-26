import pandas as pd
import os
from collections import defaultdict
from datetime import datetime
import json

def analyze_excel_sample():
    """Analyze Excel files with basic Python libraries"""
    excel_folder = "../XLSx data"
    
    print("📊 Excel Files Analysis")
    print("=" * 50)
    
    # List all Excel files
    excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
    print(f"Found {len(excel_files)} Excel files:")
    
    total_size = 0
    for i, file in enumerate(excel_files, 1):
        file_path = os.path.join(excel_folder, file)
        file_size = os.path.getsize(file_path)
        total_size += file_size
        print(f"  {i}. {file} ({file_size / (1024*1024):.1f} MB)")
    
    print(f"\nTotal size: {total_size / (1024*1024):.1f} MB")
    
    # Try to read first file structure
    try:
        first_file = os.path.join(excel_folder, excel_files[0])
        print(f"\n🔍 Analyzing structure of {excel_files[0]}...")
        
        # Read just first few rows to understand structure
        df_sample = pd.read_excel(first_file, nrows=5)
        print(f"Columns ({len(df_sample.columns)}):")
        for i, col in enumerate(df_sample.columns, 1):
            print(f"  {i:2}. {col}")
        
        print(f"\nSample data (first 3 rows):")
        print(df_sample.head(3).to_string())
        
        # Get full row count (this might take time for large files)
        print(f"\nGetting row count...")
        df_full = pd.read_excel(first_file)
        print(f"Total rows in {excel_files[0]}: {len(df_full):,}")
        
        # Analyze key columns if they exist
        key_columns = ['PENSIONER_PINCODE', 'BRANCH_PINCODE', 'YOB', 'BANK_NAME', 'BRANCH_NAME']
        available_columns = [col for col in key_columns if col in df_full.columns]
        
        print(f"\nKey columns analysis:")
        for col in available_columns:
            non_null_count = df_full[col].notna().sum()
            unique_count = df_full[col].nunique()
            print(f"  {col}: {non_null_count:,} non-null, {unique_count:,} unique values")
        
        # Age analysis if YOB exists
        if 'YOB' in df_full.columns:
            print(f"\nAge Analysis (based on YOB):")
            current_year = datetime.now().year
            df_full['AGE'] = current_year - pd.to_numeric(df_full['YOB'], errors='coerce')
            
            age_ranges = {
                'Below 60': (0, 59),
                '60-65': (60, 65),
                '66-70': (66, 70),
                '71-75': (71, 75),
                '76-80': (76, 80),
                '80+': (81, 120)
            }
            
            for age_group, (min_age, max_age) in age_ranges.items():
                count = ((df_full['AGE'] >= min_age) & (df_full['AGE'] <= max_age)).sum()
                percentage = (count / len(df_full)) * 100
                print(f"  {age_group:8}: {count:6,} ({percentage:5.1f}%)")
        
        # State analysis if pincode columns exist
        if 'BRANCH_PINCODE' in df_full.columns:
            print(f"\nState Analysis (based on BRANCH_PINCODE):")
            
            def get_state_from_pincode(pincode):
                try:
                    pin_str = str(pincode).replace('.0', '')
                    if len(pin_str) >= 3:
                        pin_num = int(pin_str[:3])
                        if 301 <= pin_num <= 345: return 'Rajasthan'
                        elif 360 <= pin_num <= 396: return 'Gujarat'
                        elif 400 <= pin_num <= 445: return 'Maharashtra'
                        elif 560 <= pin_num <= 591: return 'Karnataka'
                        elif 201 <= pin_num <= 285: return 'Uttar Pradesh'
                        elif 110 <= pin_num <= 140: return 'Delhi'
                        elif 600 <= pin_num <= 643: return 'Tamil Nadu'
                        elif 700 <= pin_num <= 743: return 'West Bengal'
                        elif 500 <= pin_num <= 509: return 'Telangana'
                        elif 800 <= pin_num <= 855: return 'Bihar'
                        else: return 'Other State'
                    return 'Invalid Pincode'
                except:
                    return 'Invalid Pincode'
            
            df_full['BANK_STATE'] = df_full['BRANCH_PINCODE'].apply(get_state_from_pincode)
            state_counts = df_full['BANK_STATE'].value_counts()
            
            print("  Top 10 states by bank verification count:")
            for i, (state, count) in enumerate(state_counts.head(10).items(), 1):
                percentage = (count / len(df_full)) * 100
                print(f"    {i:2}. {state:15}: {count:6,} ({percentage:5.1f}%)")
        
        # Bank analysis
        if 'BANK_NAME' in df_full.columns:
            print(f"\nBank Analysis:")
            bank_counts = df_full['BANK_NAME'].value_counts()
            print(f"  Total unique banks: {len(bank_counts)}")
            print("  Top 10 banks by verification count:")
            for i, (bank, count) in enumerate(bank_counts.head(10).items(), 1):
                percentage = (count / len(df_full)) * 100
                bank_short = bank[:30] + "..." if len(str(bank)) > 30 else bank
                print(f"    {i:2}. {bank_short:33}: {count:6,} ({percentage:5.1f}%)")
        
        # Save summary data
        summary_data = {
            'total_files': len(excel_files),
            'total_size_mb': round(total_size / (1024*1024), 1),
            'sample_file': excel_files[0],
            'total_rows': len(df_full),
            'columns': list(df_full.columns),
            'file_list': excel_files
        }
        
        if 'YOB' in df_full.columns:
            age_summary = {}
            for age_group, (min_age, max_age) in age_ranges.items():
                count = ((df_full['AGE'] >= min_age) & (df_full['AGE'] <= max_age)).sum()
                age_summary[age_group] = int(count)
            summary_data['age_distribution'] = age_summary
        
        if 'BANK_STATE' in df_full.columns:
            summary_data['state_distribution'] = state_counts.head(10).to_dict()
        
        if 'BANK_NAME' in df_full.columns:
            summary_data['bank_distribution'] = bank_counts.head(10).to_dict()
        
        # Save to JSON
        with open('excel_files_summary.json', 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        print(f"\n💾 Summary saved to: excel_files_summary.json")
        
    except Exception as e:
        print(f"❌ Error analyzing Excel files: {e}")
        print("This might be due to missing pandas or openpyxl libraries")
        
        # Fallback: Just show file information
        print(f"\n📁 File Information Only:")
        for i, file in enumerate(excel_files, 1):
            file_path = os.path.join(excel_folder, file)
            file_size = os.path.getsize(file_path)
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            print(f"  {i}. {file}")
            print(f"     Size: {file_size / (1024*1024):.1f} MB")
            print(f"     Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    analyze_excel_sample()
