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
        pin_str = str(pincode).replace('.0', '')
        if len(pin_str) >= 3:
            pin_num = int(pin_str[:3])
            
            if 110 <= pin_num <= 140: return 'Delhi'
            elif 201 <= pin_num <= 285: return 'Uttar Pradesh'
            elif 301 <= pin_num <= 345: return 'Rajasthan'
            elif 360 <= pin_num <= 396: return 'Gujarat'
            elif 400 <= pin_num <= 445: return 'Maharashtra'
            elif 500 <= pin_num <= 509: return 'Telangana'
            elif 510 <= pin_num <= 518: return 'Andhra Pradesh'
            elif 560 <= pin_num <= 591: return 'Karnataka'
            elif 600 <= pin_num <= 643: return 'Tamil Nadu'
            elif 682 <= pin_num <= 695: return 'Kerala'
            elif 700 <= pin_num <= 743: return 'West Bengal'
            elif 751 <= pin_num <= 770: return 'Odisha'
            elif 800 <= pin_num <= 855: return 'Bihar'
            elif 781 <= pin_num <= 788: return 'Assam'
            elif 160 <= pin_num <= 165: return 'Punjab'
            elif 171 <= pin_num <= 177: return 'Himachal Pradesh'
            elif 180 <= pin_num <= 194: return 'Jammu and Kashmir'
            else: return 'Other State'
        return 'Invalid Pincode'
    except:
        return 'Invalid Pincode'

def analyze_dlc_by_bank_pincode():
    """Analyze DLC completion by bank pincode with age-wise distribution"""
    excel_folder = "../XLSx data"
    
    print("🏦 DLC COMPLETION ANALYSIS BY BANK PINCODE")
    print("=" * 70)
    print("📊 Processing 4+ million pensioner records...")
    print("🎯 Analyzing: Bank Pincode → Age Distribution → DLC Count")
    print("-" * 70)
    
    # Initialize data structures
    bank_pincode_data = defaultdict(lambda: {
        'total_dlc_completed': 0,
        'age_groups': defaultdict(int),
        'state': '',
        'pensioner_states': defaultdict(int)
    })
    
    total_processed = 0
    excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
    
    # Process each Excel file
    for file_index, excel_file in enumerate(excel_files, 1):
        file_path = os.path.join(excel_folder, excel_file)
        print(f"📖 Processing File {file_index}/{len(excel_files)}: {excel_file}")
        
        try:
            # Read Excel file
            print(f"   📖 Loading file into memory...")
            df = pd.read_excel(file_path)
            print(f"   ✅ Loaded {len(df):,} records")
            
            file_records = 0
            
            # Process records
            for index, row in df.iterrows():
                try:
                    # Extract data
                    branch_pincode = str(row.get('BRANCH_PINCODE', '')).replace('.0', '') if pd.notna(row.get('BRANCH_PINCODE')) else ''
                    pensioner_pincode = str(row.get('PENSIONER_PINCODE', '')).replace('.0', '') if pd.notna(row.get('PENSIONER_PINCODE')) else ''
                    birth_year = int(float(row.get('YOB', 1960))) if pd.notna(row.get('YOB')) else 1960
                    
                    # Skip invalid data
                    if not branch_pincode or len(branch_pincode) < 6:
                        continue
                    
                    # Calculate age group
                    age_group = get_age_group(birth_year)
                    
                    # Get locations
                    bank_state = get_state_from_pincode(branch_pincode)
                    pensioner_state = get_state_from_pincode(pensioner_pincode)
                    
                    # Update bank pincode data
                    bank_data = bank_pincode_data[branch_pincode]
                    bank_data['total_dlc_completed'] += 1
                    bank_data['age_groups'][age_group] += 1
                    bank_data['pensioner_states'][pensioner_state] += 1
                    
                    # Set location info (first time)
                    if not bank_data['state']:
                        bank_data['state'] = bank_state
                    
                    file_records += 1
                    total_processed += 1
                    
                    # Progress update every 100k records
                    if total_processed % 100000 == 0:
                        print(f"   ✅ Processed {total_processed:,} total records...")
                    
                except Exception as e:
                    continue  # Skip problematic records
            
            print(f"   ✅ File completed: {file_records:,} records processed")
            
        except Exception as e:
            print(f"   ❌ Error processing {excel_file}: {e}")
            continue
    
    print(f"\n🎯 Processing Complete!")
    print(f"📊 Total Records Processed: {total_processed:,}")
    print(f"🏦 Unique Bank Pincodes Found: {len(bank_pincode_data):,}")
    
    # Convert to regular dict for JSON serialization
    final_data = {}
    for pincode, data in bank_pincode_data.items():
        if data['total_dlc_completed'] > 0:  # Only include pincodes with DLC completions
            final_data[pincode] = {
                'total_dlc_completed': data['total_dlc_completed'],
                'age_groups': dict(data['age_groups']),
                'state': data['state'],
                'pensioner_states': dict(data['pensioner_states'])
            }
    
    # Generate analysis report
    generate_dlc_analysis_report(final_data, total_processed)
    
    # Save detailed data
    save_dlc_analysis_data(final_data, total_processed)
    
    return final_data

def generate_dlc_analysis_report(bank_data, total_processed):
    """Generate comprehensive DLC analysis report"""
    print("\n" + "=" * 70)
    print("📊 DLC COMPLETION ANALYSIS REPORT")
    print("=" * 70)
    
    # Summary statistics
    total_bank_pincodes = len(bank_data)
    total_dlc_completed = sum(data['total_dlc_completed'] for data in bank_data.values())
    
    print(f"📈 SUMMARY STATISTICS:")
    print(f"   Total Records Processed: {total_processed:,}")
    print(f"   Total DLC Completed: {total_dlc_completed:,}")
    print(f"   Unique Bank Pincodes: {total_bank_pincodes:,}")
    print(f"   Average DLC per Bank: {total_dlc_completed // total_bank_pincodes if total_bank_pincodes > 0 else 0:,}")
    
    # Top 15 bank pincodes by DLC completion
    print(f"\n🏆 TOP 15 BANK PINCODES BY DLC COMPLETION:")
    top_pincodes = sorted(bank_data.items(), key=lambda x: x[1]['total_dlc_completed'], reverse=True)[:15]
    
    for i, (pincode, data) in enumerate(top_pincodes, 1):
        percentage = (data['total_dlc_completed'] / total_dlc_completed) * 100
        top_age_group = max(data['age_groups'].items(), key=lambda x: x[1])[0] if data['age_groups'] else 'Unknown'
        top_age_count = max(data['age_groups'].values()) if data['age_groups'] else 0
        print(f"   {i:2}. Pincode {pincode} ({data['state']})")
        print(f"       DLC Completed: {data['total_dlc_completed']:,} ({percentage:.2f}%)")
        print(f"       Top Age Group: {top_age_group} ({top_age_count:,} pensioners)")
    
    # State-wise analysis
    print(f"\n🗺️ STATE-WISE DLC COMPLETION ANALYSIS:")
    state_totals = defaultdict(int)
    state_pincodes = defaultdict(int)
    
    for pincode, data in bank_data.items():
        state_totals[data['state']] += data['total_dlc_completed']
        state_pincodes[data['state']] += 1
    
    state_sorted = sorted(state_totals.items(), key=lambda x: x[1], reverse=True)[:10]
    for i, (state, total) in enumerate(state_sorted, 1):
        percentage = (total / total_dlc_completed) * 100
        avg_per_pincode = total // state_pincodes[state] if state_pincodes[state] > 0 else 0
        print(f"   {i:2}. {state:20}: {total:8,} DLC ({percentage:5.1f}%) | {state_pincodes[state]:3} pincodes | Avg: {avg_per_pincode:,}")
    
    # Age group analysis across all bank pincodes
    print(f"\n👥 OVERALL AGE GROUP DISTRIBUTION:")
    overall_age_groups = defaultdict(int)
    for data in bank_data.values():
        for age_group, count in data['age_groups'].items():
            overall_age_groups[age_group] += count
    
    age_sorted = sorted(overall_age_groups.items(), key=lambda x: x[1], reverse=True)
    for age_group, count in age_sorted:
        percentage = (count / total_dlc_completed) * 100
        print(f"   {age_group:12}: {count:8,} ({percentage:5.1f}%)")

def save_dlc_analysis_data(bank_data, total_processed):
    """Save DLC analysis data to files"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. Complete bank pincode analysis
    complete_data = {
        'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_records_processed': total_processed,
        'total_bank_pincodes': len(bank_data),
        'total_dlc_completed': sum(data['total_dlc_completed'] for data in bank_data.values()),
        'bank_pincode_data': bank_data
    }
    
    with open(f'dlc_bank_analysis_{timestamp}.json', 'w') as f:
        json.dump(complete_data, f, indent=2)
    
    # 2. Top performing bank pincodes
    top_pincodes = sorted(bank_data.items(), key=lambda x: x[1]['total_dlc_completed'], reverse=True)[:50]
    top_data = {
        'top_50_bank_pincodes': {
            pincode: data for pincode, data in top_pincodes
        }
    }
    
    with open(f'top_bank_pincodes_{timestamp}.json', 'w') as f:
        json.dump(top_data, f, indent=2)
    
    print(f"\n💾 Analysis data saved:")
    print(f"   📄 dlc_bank_analysis_{timestamp}.json (Complete data)")
    print(f"   🏆 top_bank_pincodes_{timestamp}.json (Top 50 performers)")

if __name__ == "__main__":
    print("🚀 Starting DLC Bank Pincode Analysis...")
    print("⚠️  Processing 4+ million records - this may take several minutes")
    
    try:
        result = analyze_dlc_by_bank_pincode()
        print(f"\n✅ Analysis Complete!")
        print(f"🎯 Found DLC completion data for {len(result):,} unique bank pincodes")
        
    except ImportError as e:
        print(f"❌ Missing required library: {e}")
        print("💡 Please install: pip install pandas openpyxl")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
