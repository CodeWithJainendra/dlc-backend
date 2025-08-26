import pandas as pd
import os
from collections import defaultdict
from datetime import datetime
import json

def get_age_group(birth_year):
    """Calculate age group from birth year"""
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

def get_state_from_pincode(pincode):
    """Get state from pincode"""
    try:
        pin_num = int(str(pincode)[:3])
        
        # State mappings based on pincode ranges
        if 110 <= pin_num <= 140:
            return 'Delhi'
        elif 201 <= pin_num <= 285:
            return 'Uttar Pradesh'
        elif 301 <= pin_num <= 345:
            return 'Rajasthan'
        elif 360 <= pin_num <= 396:
            return 'Gujarat'
        elif 400 <= pin_num <= 445:
            return 'Maharashtra'
        elif 500 <= pin_num <= 509:
            return 'Telangana'
        elif 560 <= pin_num <= 591:
            return 'Karnataka'
        elif 600 <= pin_num <= 643:
            return 'Tamil Nadu'
        elif 700 <= pin_num <= 743:
            return 'West Bengal'
        elif 800 <= pin_num <= 855:
            return 'Bihar'
        else:
            return 'Other State'
    except:
        return 'Unknown State'

def analyze_excel_files():
    """Analyze all 5 Excel files and extract comprehensive data"""
    excel_folder = "../XLSx data"
    
    # Initialize data structures
    pensioner_data = []
    bank_data = defaultdict(lambda: {
        'total_pensioners': 0,
        'locations': set(),
        'states': set(),
        'pincodes': set()
    })
    age_group_data = defaultdict(int)
    state_wise_data = defaultdict(lambda: {
        'total_pensioners': 0,
        'age_groups': defaultdict(int),
        'bank_locations': defaultdict(int),
        'pincode_counts': defaultdict(int),
        'pensioner_details': []
    })
    
    excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
    total_records = 0
    
    print(f"📊 Found {len(excel_files)} Excel files to analyze:")
    for i, file in enumerate(excel_files, 1):
        print(f"  {i}. {file}")
    
    # Process each Excel file
    for file_index, excel_file in enumerate(excel_files):
        file_path = os.path.join(excel_folder, excel_file)
        print(f"\n🔄 Processing file {file_index + 1}/{len(excel_files)}: {excel_file}")
        
        try:
            # Read Excel file (limit rows for faster processing)
            df = pd.read_excel(file_path, nrows=10000)  # Analyze first 10k rows per file
            print(f"📋 Columns: {list(df.columns)}")
            print(f"📊 Shape: {df.shape}")
            
            file_records = 0
            
            for index, row in df.iterrows():
                try:
                    # Extract key fields
                    pensioner_pincode = str(row.get('PENSIONER_PINCODE', '')).replace('.0', '') if pd.notna(row.get('PENSIONER_PINCODE')) else ''
                    branch_pincode = str(row.get('BRANCH_PINCODE', '')).replace('.0', '') if pd.notna(row.get('BRANCH_PINCODE')) else ''
                    birth_year = int(float(row.get('YOB', 1960))) if pd.notna(row.get('YOB')) else 1960
                    bank_name = str(row.get('BANK_NAME', 'Unknown Bank')) if pd.notna(row.get('BANK_NAME')) else 'Unknown Bank'
                    branch_name = str(row.get('BRANCH_NAME', 'Unknown Branch')) if pd.notna(row.get('BRANCH_NAME')) else 'Unknown Branch'
                    
                    # Calculate age and age group
                    age_group = get_age_group(birth_year)
                    current_age = datetime.now().year - birth_year
                    
                    # Get states
                    pensioner_state = get_state_from_pincode(pensioner_pincode)
                    bank_state = get_state_from_pincode(branch_pincode)
                    
                    # Create pensioner record
                    pensioner_record = {
                        'pensioner_pincode': pensioner_pincode,
                        'pensioner_state': pensioner_state,
                        'branch_pincode': branch_pincode,
                        'bank_state': bank_state,
                        'birth_year': birth_year,
                        'current_age': current_age,
                        'age_group': age_group,
                        'bank_name': bank_name,
                        'branch_name': branch_name,
                        'file_source': excel_file
                    }
                    
                    pensioner_data.append(pensioner_record)
                    
                    # Update bank data
                    bank_key = f"{bank_name} - {branch_name}"
                    bank_data[bank_key]['total_pensioners'] += 1
                    bank_data[bank_key]['locations'].add(f"{branch_name}, {bank_state}")
                    bank_data[bank_key]['states'].add(bank_state)
                    bank_data[bank_key]['pincodes'].add(branch_pincode)
                    
                    # Update age group data
                    age_group_data[age_group] += 1
                    
                    # Update state-wise data (based on bank verification location)
                    state_wise_data[bank_state]['total_pensioners'] += 1
                    state_wise_data[bank_state]['age_groups'][age_group] += 1
                    state_wise_data[bank_state]['bank_locations'][f"{bank_name} - {branch_name}"] += 1
                    state_wise_data[bank_state]['pincode_counts'][branch_pincode] += 1
                    state_wise_data[bank_state]['pensioner_details'].append(pensioner_record)
                    
                    file_records += 1
                    total_records += 1
                    
                except Exception as e:
                    print(f"❌ Error processing row {index}: {e}")
                    continue
            
            print(f"✅ Processed {file_records} records from {excel_file}")
            
        except Exception as e:
            print(f"❌ Error reading {excel_file}: {e}")
            continue
    
    print(f"\n🎯 Analysis Complete!")
    print(f"📊 Total Records Processed: {total_records:,}")
    print(f"🏦 Unique Banks Found: {len(bank_data)}")
    print(f"🗺️ States with Data: {len(state_wise_data)}")
    
    return {
        'pensioner_data': pensioner_data,
        'bank_data': dict(bank_data),
        'age_group_data': dict(age_group_data),
        'state_wise_data': dict(state_wise_data),
        'total_records': total_records,
        'files_processed': len(excel_files)
    }

def generate_analysis_report(analysis_data):
    """Generate comprehensive analysis report"""
    print("\n" + "="*80)
    print("📊 COMPREHENSIVE EXCEL DATA ANALYSIS REPORT")
    print("="*80)
    
    # Summary Statistics
    print(f"\n📈 SUMMARY STATISTICS:")
    print(f"   Total Pensioners Analyzed: {analysis_data['total_records']:,}")
    print(f"   Files Processed: {analysis_data['files_processed']}")
    print(f"   Unique Banks: {len(analysis_data['bank_data'])}")
    print(f"   States with Verification Data: {len(analysis_data['state_wise_data'])}")
    
    # Age Group Analysis
    print(f"\n👥 AGE GROUP DISTRIBUTION:")
    age_groups = sorted(analysis_data['age_group_data'].items(), key=lambda x: x[1], reverse=True)
    for age_group, count in age_groups:
        percentage = (count / analysis_data['total_records']) * 100
        print(f"   {age_group:12}: {count:6,} ({percentage:5.1f}%)")
    
    # State-wise Analysis
    print(f"\n🗺️ TOP 10 STATES BY DLC VERIFICATIONS:")
    state_totals = [(state, data['total_pensioners']) for state, data in analysis_data['state_wise_data'].items()]
    state_totals.sort(key=lambda x: x[1], reverse=True)
    
    for i, (state, count) in enumerate(state_totals[:10], 1):
        percentage = (count / analysis_data['total_records']) * 100
        print(f"   {i:2}. {state:20}: {count:6,} ({percentage:5.1f}%)")
    
    # Bank Analysis
    print(f"\n🏦 TOP 10 BANKS BY VERIFICATION COUNT:")
    bank_totals = [(bank, data['total_pensioners']) for bank, data in analysis_data['bank_data'].items()]
    bank_totals.sort(key=lambda x: x[1], reverse=True)
    
    for i, (bank, count) in enumerate(bank_totals[:10], 1):
        percentage = (count / analysis_data['total_records']) * 100
        bank_short = bank[:50] + "..." if len(bank) > 50 else bank
        print(f"   {i:2}. {bank_short:53}: {count:6,} ({percentage:5.1f}%)")
    
    # Detailed State Analysis
    print(f"\n🔍 DETAILED STATE ANALYSIS:")
    for state, data in sorted(analysis_data['state_wise_data'].items(), key=lambda x: x[1]['total_pensioners'], reverse=True)[:5]:
        print(f"\n   📍 {state}:")
        print(f"      Total Verifications: {data['total_pensioners']:,}")
        print(f"      Unique Banks: {len(data['bank_locations'])}")
        print(f"      Unique Pincodes: {len(data['pincode_counts'])}")
        
        # Top age groups in this state
        top_ages = sorted(data['age_groups'].items(), key=lambda x: x[1], reverse=True)[:3]
        print(f"      Top Age Groups: {', '.join([f'{age}({count})' for age, count in top_ages])}")

def save_analysis_to_files(analysis_data):
    """Save analysis data to JSON files"""
    output_dir = "../backend"
    
    # Save comprehensive data
    with open(os.path.join(output_dir, 'excel_analysis_complete.json'), 'w') as f:
        # Convert sets to lists for JSON serialization
        serializable_data = {}
        for key, value in analysis_data.items():
            if key == 'bank_data':
                serializable_data[key] = {}
                for bank, bank_info in value.items():
                    serializable_data[key][bank] = {
                        'total_pensioners': bank_info['total_pensioners'],
                        'locations': list(bank_info['locations']),
                        'states': list(bank_info['states']),
                        'pincodes': list(bank_info['pincodes'])
                    }
            elif key == 'state_wise_data':
                serializable_data[key] = {}
                for state, state_info in value.items():
                    serializable_data[key][state] = {
                        'total_pensioners': state_info['total_pensioners'],
                        'age_groups': dict(state_info['age_groups']),
                        'bank_locations': dict(state_info['bank_locations']),
                        'pincode_counts': dict(state_info['pincode_counts']),
                        'pensioner_count': len(state_info['pensioner_details'])
                    }
            else:
                serializable_data[key] = value
        
        json.dump(serializable_data, f, indent=2)
    
    # Save filtered datasets
    
    # 1. Age-wise filtered data
    age_filtered = {}
    for age_group in ['60-65', '66-70', '71-75', '76-80', '80+']:
        age_filtered[age_group] = [
            p for p in analysis_data['pensioner_data'] 
            if p['age_group'] == age_group
        ]
    
    with open(os.path.join(output_dir, 'age_wise_filtered.json'), 'w') as f:
        json.dump(age_filtered, f, indent=2)
    
    # 2. Bank-wise filtered data
    bank_filtered = {}
    for bank, bank_info in analysis_data['bank_data'].items():
        if bank_info['total_pensioners'] >= 100:  # Only banks with 100+ verifications
            bank_filtered[bank] = {
                'total_pensioners': bank_info['total_pensioners'],
                'locations': list(bank_info['locations']),
                'states': list(bank_info['states']),
                'pincodes': list(bank_info['pincodes'])
            }
    
    with open(os.path.join(output_dir, 'bank_wise_filtered.json'), 'w') as f:
        json.dump(bank_filtered, f, indent=2)
    
    # 3. State-wise summary
    state_summary = {}
    for state, data in analysis_data['state_wise_data'].items():
        state_summary[state] = {
            'total_pensioners': data['total_pensioners'],
            'top_age_group': max(data['age_groups'].items(), key=lambda x: x[1])[0] if data['age_groups'] else 'Unknown',
            'unique_banks': len(data['bank_locations']),
            'unique_pincodes': len(data['pincode_counts']),
            'age_distribution': dict(data['age_groups'])
        }
    
    with open(os.path.join(output_dir, 'state_wise_summary.json'), 'w') as f:
        json.dump(state_summary, f, indent=2)
    
    print(f"\n💾 Analysis data saved to:")
    print(f"   - excel_analysis_complete.json (Full dataset)")
    print(f"   - age_wise_filtered.json (Age group filters)")
    print(f"   - bank_wise_filtered.json (Bank filters)")
    print(f"   - state_wise_summary.json (State summaries)")

if __name__ == "__main__":
    print("🚀 Starting Excel Data Analysis...")
    
    # Analyze all Excel files
    analysis_data = analyze_excel_files()
    
    # Generate comprehensive report
    generate_analysis_report(analysis_data)
    
    # Save analysis to files
    save_analysis_to_files(analysis_data)
    
    print(f"\n✅ Analysis Complete! Check the generated JSON files for detailed data.")
