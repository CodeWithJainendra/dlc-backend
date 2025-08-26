import os
from datetime import datetime

def count_excel_data_simple():
    """Simple data counter that works without pandas"""
    excel_folder = "../XLSx data"
    
    print("🔢 EXCEL FILES DATA COUNTER")
    print("=" * 50)
    
    # Get all Excel files
    try:
        excel_files = [f for f in os.listdir(excel_folder) if f.endswith('.xlsx')]
        excel_files.sort()
    except FileNotFoundError:
        print(f"❌ Folder not found: {excel_folder}")
        return
    
    if not excel_files:
        print("❌ No Excel files found!")
        return
    
    print(f"📁 Found {len(excel_files)} Excel files:")
    print("-" * 50)
    
    total_size = 0
    file_info = []
    
    # Analyze each file
    for i, excel_file in enumerate(excel_files, 1):
        file_path = os.path.join(excel_folder, excel_file)
        
        try:
            # Get file statistics
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            total_size += file_size
            
            # Get modification time
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            file_data = {
                'name': excel_file,
                'size_mb': round(file_size_mb, 1),
                'size_bytes': file_size,
                'modified': mod_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            file_info.append(file_data)
            
            print(f"{i}. {excel_file}")
            print(f"   Size: {file_size_mb:.1f} MB")
            print(f"   Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            print(f"{i}. {excel_file} - ERROR: {e}")
    
    # Summary
    total_size_mb = total_size / (1024 * 1024)
    print("\n" + "=" * 50)
    print("📊 SUMMARY")
    print("=" * 50)
    print(f"📁 Total Files: {len(excel_files)}")
    print(f"💾 Total Size: {total_size_mb:.1f} MB")
    print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Estimate record counts based on file sizes
    print(f"\n📊 ESTIMATED RECORD COUNTS:")
    print("-" * 50)
    print("(Based on typical Excel file size patterns)")
    
    estimated_total = 0
    for info in file_info:
        # Rough estimate: 1MB ≈ 3000-5000 records for typical pensioner data
        estimated_records = int(info['size_mb'] * 4000)  # Using 4000 as middle estimate
        estimated_total += estimated_records
        print(f"{info['name'][:30]:30} ~{estimated_records:,} records")
    
    print("-" * 50)
    print(f"ESTIMATED TOTAL RECORDS: ~{estimated_total:,}")
    
    # Try with pandas if available
    try:
        import pandas as pd
        print(f"\n🔍 ACTUAL COUNT (using pandas):")
        print("-" * 50)
        
        actual_total = 0
        for i, excel_file in enumerate(excel_files, 1):
            file_path = os.path.join(excel_folder, excel_file)
            try:
                print(f"Reading {excel_file}... ", end="")
                df = pd.read_excel(file_path)
                record_count = len(df)
                actual_total += record_count
                print(f"{record_count:,} records")
                
                # Show column info for first file
                if i == 1:
                    print(f"   Columns ({len(df.columns)}): {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
                
            except Exception as e:
                print(f"Error: {e}")
        
        print("-" * 50)
        print(f"ACTUAL TOTAL RECORDS: {actual_total:,}")
        
        # Compare estimate vs actual
        if estimated_total > 0 and actual_total > 0:
            accuracy = (min(estimated_total, actual_total) / max(estimated_total, actual_total)) * 100
            print(f"Estimation Accuracy: {accuracy:.1f}%")
        
    except ImportError:
        print(f"\n💡 For exact record counts, install pandas:")
        print("   pip install pandas openpyxl")
    except Exception as e:
        print(f"\n❌ Error getting actual counts: {e}")
    
    print(f"\n✅ Analysis Complete!")

if __name__ == "__main__":
    count_excel_data_simple()
