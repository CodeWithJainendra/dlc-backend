import pandas as pd
import os
from datetime import datetime
import json

def count_total_data_from_excel_files():
    """Count total number of records from all 5 Excel files"""
    excel_folder = "../XLSx data"
    
    print("🔢 TOTAL DATA COUNTER - Excel Files Analysis")
    print("=" * 60)
    
    # Initialize counters
    total_records = 0
    file_details = []
    
    # Get all Excel files
    excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
    excel_files.sort()  # Sort for consistent order
    
    print(f"📁 Found {len(excel_files)} Excel files:")
    print("-" * 60)
    
    # Process each file
    for i, excel_file in enumerate(excel_files, 1):
        file_path = os.path.join(excel_folder, excel_file)
        
        try:
            print(f"📊 Processing File {i}: {excel_file}")
            
            # Get file size
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            # Read Excel file to count rows
            print(f"   📖 Reading file... (Size: {file_size_mb:.1f} MB)")
            df = pd.read_excel(file_path)
            
            # Count records
            file_record_count = len(df)
            total_records += file_record_count
            
            # Get file modification date
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            # Store file details
            file_info = {
                'file_name': excel_file,
                'record_count': file_record_count,
                'file_size_mb': round(file_size_mb, 1),
                'columns': list(df.columns),
                'column_count': len(df.columns),
                'modified_date': mod_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            file_details.append(file_info)
            
            print(f"   ✅ Records: {file_record_count:,}")
            print(f"   📋 Columns: {len(df.columns)}")
            print(f"   📅 Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
        except Exception as e:
            print(f"   ❌ Error reading {excel_file}: {e}")
            file_info = {
                'file_name': excel_file,
                'record_count': 0,
                'file_size_mb': round(file_size_mb, 1),
                'error': str(e),
                'modified_date': mod_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            file_details.append(file_info)
            print()
    
    # Summary Report
    print("=" * 60)
    print("📊 TOTAL DATA SUMMARY")
    print("=" * 60)
    
    total_size_mb = sum([info['file_size_mb'] for info in file_details])
    successful_files = len([info for info in file_details if 'error' not in info])
    
    print(f"📁 Total Files Processed: {len(excel_files)}")
    print(f"✅ Successfully Read: {successful_files}")
    print(f"📊 Total Records: {total_records:,}")
    print(f"💾 Total Size: {total_size_mb:.1f} MB")
    print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n📋 File-wise Breakdown:")
    print("-" * 60)
    for i, info in enumerate(file_details, 1):
        if 'error' not in info:
            percentage = (info['record_count'] / total_records * 100) if total_records > 0 else 0
            print(f"{i}. {info['file_name']}")
            print(f"   Records: {info['record_count']:,} ({percentage:.1f}%)")
            print(f"   Size: {info['file_size_mb']} MB")
            print(f"   Columns: {info['column_count']}")
        else:
            print(f"{i}. {info['file_name']} - ERROR: {info['error']}")
    
    # Check for common columns across files
    if successful_files > 1:
        print(f"\n🔍 Column Analysis:")
        print("-" * 60)
        
        all_columns = []
        for info in file_details:
            if 'columns' in info:
                all_columns.extend(info['columns'])
        
        # Find common columns
        from collections import Counter
        column_counts = Counter(all_columns)
        common_columns = [col for col, count in column_counts.items() if count == successful_files]
        
        print(f"📋 Common columns across all files ({len(common_columns)}):")
        for col in common_columns:
            print(f"   • {col}")
        
        if len(common_columns) < len(column_counts):
            unique_columns = [col for col, count in column_counts.items() if count < successful_files]
            print(f"\n📋 Unique/Different columns ({len(unique_columns)}):")
            for col in unique_columns[:10]:  # Show first 10
                files_with_col = column_counts[col]
                print(f"   • {col} (in {files_with_col}/{successful_files} files)")
    
    # Save detailed results
    summary_data = {
        'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_files': len(excel_files),
        'successful_files': successful_files,
        'total_records': total_records,
        'total_size_mb': round(total_size_mb, 1),
        'file_details': file_details,
        'common_columns': common_columns if successful_files > 1 else [],
        'summary_stats': {
            'avg_records_per_file': round(total_records / successful_files) if successful_files > 0 else 0,
            'largest_file': max(file_details, key=lambda x: x.get('record_count', 0))['file_name'] if file_details else None,
            'smallest_file': min(file_details, key=lambda x: x.get('record_count', float('inf')))['file_name'] if file_details else None
        }
    }
    
    # Save to JSON
    output_file = 'total_data_analysis.json'
    with open(output_file, 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    print(f"\n💾 Detailed analysis saved to: {output_file}")
    
    # Quick data validation if possible
    if total_records > 0:
        print(f"\n🔍 Quick Data Validation:")
        print("-" * 60)
        
        # Sample first file for data quality check
        try:
            first_file_path = os.path.join(excel_folder, excel_files[0])
            sample_df = pd.read_excel(first_file_path, nrows=1000)  # Sample first 1000 rows
            
            # Check for key columns
            key_columns = ['PENSIONER_PINCODE', 'BRANCH_PINCODE', 'YOB', 'BANK_NAME']
            available_key_columns = [col for col in key_columns if col in sample_df.columns]
            
            print(f"📋 Key columns found: {len(available_key_columns)}/{len(key_columns)}")
            for col in available_key_columns:
                non_null_count = sample_df[col].notna().sum()
                null_percentage = ((1000 - non_null_count) / 1000) * 100
                print(f"   • {col}: {non_null_count}/1000 non-null ({null_percentage:.1f}% null)")
            
        except Exception as e:
            print(f"   ❌ Could not perform data validation: {e}")
    
    print(f"\n✅ Analysis Complete!")
    print(f"🎯 Total Pensioner Records Found: {total_records:,}")
    
    return summary_data

if __name__ == "__main__":
    try:
        result = count_total_data_from_excel_files()
    except ImportError as e:
        print(f"❌ Missing required library: {e}")
        print("💡 Please install required libraries:")
        print("   pip install pandas openpyxl")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
