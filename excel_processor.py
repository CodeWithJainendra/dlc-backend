#!/usr/bin/env python3
"""
Excel Data Processor for Pensioner Data
Processes GAD_DLC_PINCODE_DATA Excel files to extract pensioner information
"""

import pandas as pd
import os
import json
from typing import Dict, List, Any
from datetime import datetime
import sqlite3

class ExcelDataProcessor:
    def __init__(self, excel_folder_path: str):
        self.excel_folder_path = excel_folder_path
        self.processed_data = {}
        self.state_pincode_mapping = {}
        
    def load_pincode_state_mapping(self):
        """Load pincode to state mapping from existing JSON files"""
        try:
            # Load existing pincode data if available
            with open('../extracted_pincodes.json', 'r') as f:
                pincode_data = json.load(f)
                for entry in pincode_data:
                    if 'pincode' in entry and 'state' in entry:
                        self.state_pincode_mapping[str(entry['pincode'])] = entry['state']
        except FileNotFoundError:
            print("Pincode mapping file not found, will use basic state detection")
    
    def detect_state_from_pincode(self, pincode: str) -> str:
        """Detect state from pincode using comprehensive mapping"""
        if pincode in self.state_pincode_mapping:
            return self.state_pincode_mapping[pincode]
        
        # Comprehensive state detection based on pincode ranges
        pincode_num = int(pincode[:3]) if pincode and len(pincode) >= 3 and pincode[:3].isdigit() else 0
        
        if 110000 <= pincode_num <= 140000:
            return 'Delhi'
        elif 121000 <= pincode_num <= 136000:
            return 'Haryana'
        elif 140000 <= pincode_num <= 160000:
            return 'Punjab'
        elif 301000 <= pincode_num <= 345000:
            return 'Rajasthan'
        elif 201000 <= pincode_num <= 285000:
            return 'Uttar Pradesh'
        elif 800000 <= pincode_num <= 855000:
            return 'Bihar'
        elif 700000 <= pincode_num <= 743000:
            return 'West Bengal'
        elif 400000 <= pincode_num <= 445000:
            return 'Maharashtra'
        elif 380000 <= pincode_num <= 396000:
            return 'Gujarat'
        elif 560000 <= pincode_num <= 591000:
            return 'Karnataka'
        elif 600000 <= pincode_num <= 643000:
            return 'Tamil Nadu'
        elif 500000 <= pincode_num <= 509000:
            return 'Telangana'
        elif 515000 <= pincode_num <= 535000:
            return 'Andhra Pradesh'
        elif 450000 <= pincode_num <= 492000:
            return 'Madhya Pradesh'
        elif 751000 <= pincode_num <= 770000:
            return 'Odisha'
        elif 781000 <= pincode_num <= 788000:
            return 'Assam'
        elif 682000 <= pincode_num <= 695000:
            return 'Kerala'
        elif 831000 <= pincode_num <= 835000:
            return 'Jharkhand'
        elif 248000 <= pincode_num <= 263000:
            return 'Uttarakhand'
        elif 171000 <= pincode_num <= 177000:
            return 'Himachal Pradesh'
        else:
            return 'Other States'
    
    def categorize_age_group(self, birth_year: int) -> str:
        """Categorize pensioner into age groups based on birth year"""
        current_year = datetime.now().year
        age = current_year - birth_year
        
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
    
    def process_excel_files(self) -> Dict[str, Any]:
        """Process all Excel files in the folder"""
        all_data = []
        
        # Get all Excel files
        excel_files = [f for f in os.listdir(self.excel_folder_path) if f.endswith('.xlsx')]
        
        print(f"Found {len(excel_files)} Excel files to process...")
        
        for file_name in excel_files:
            file_path = os.path.join(self.excel_folder_path, file_name)
            print(f"Processing {file_name}...")
            
            try:
                # Read Excel file
                df = pd.read_excel(file_path)
                
                # Print column names to understand structure
                print(f"Columns in {file_name}: {list(df.columns)}")
                
                # Process each row
                for index, row in df.iterrows():
                    # Extract data based on expected column names
                    # Adjust column names based on actual Excel structure
                    pensioner_data = {
                        'file_source': file_name,
                        'row_index': index
                    }
                    
                    # Extract data based on actual column structure
                    if 'PENSIONER_PINCODE' in df.columns:
                        pensioner_data['pensioner_pincode'] = str(row['PENSIONER_PINCODE']) if pd.notna(row['PENSIONER_PINCODE']) else ''
                    if 'BRANCH_PINCODE' in df.columns:
                        pensioner_data['bank_pincode'] = str(row['BRANCH_PINCODE']) if pd.notna(row['BRANCH_PINCODE']) else ''
                    if 'YOB' in df.columns:
                        pensioner_data['year'] = int(row['YOB']) if pd.notna(row['YOB']) and str(row['YOB']).replace('.', '').isdigit() else 1960
                    
                    # Set defaults if not found
                    if 'pensioner_pincode' not in pensioner_data:
                        pensioner_data['pensioner_pincode'] = ''
                    if 'bank_pincode' not in pensioner_data:
                        pensioner_data['bank_pincode'] = ''
                    if 'year' not in pensioner_data:
                        pensioner_data['year'] = 2020
                    if 'bank_name' not in pensioner_data:
                        pensioner_data['bank_name'] = 'Unknown Bank'
                    
                    all_data.append(pensioner_data)
                
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
                continue
        
        print(f"Processed {len(all_data)} total records")
        return self.analyze_data(all_data)
    
    def analyze_data(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze processed data to create state-wise statistics"""
        state_analysis = {}
        age_group_analysis = {}
        bank_analysis = {}
        
        for record in data:
            pensioner_pincode = record.get('pensioner_pincode', '')
            bank_pincode = record.get('bank_pincode', '')
            year = record.get('year', 2020)
            bank_name = record.get('bank_name', 'Unknown Bank')
            
            # Determine state from pensioner pincode
            state = self.detect_state_from_pincode(pensioner_pincode)
            age_group = self.categorize_age_group(year)
            
            # Initialize state data
            if state not in state_analysis:
                state_analysis[state] = {
                    'total_pensioners': 0,
                    'age_groups': {},
                    'banks': {},
                    'verification_locations': []
                }
            
            # Update state statistics
            state_analysis[state]['total_pensioners'] += 1
            
            # Age group analysis
            if age_group not in state_analysis[state]['age_groups']:
                state_analysis[state]['age_groups'][age_group] = 0
            state_analysis[state]['age_groups'][age_group] += 1
            
            # Bank analysis
            if bank_name not in state_analysis[state]['banks']:
                state_analysis[state]['banks'][bank_name] = 0
            state_analysis[state]['banks'][bank_name] += 1
            
            # Global age group analysis
            if age_group not in age_group_analysis:
                age_group_analysis[age_group] = 0
            age_group_analysis[age_group] += 1
            
            # Global bank analysis
            if bank_name not in bank_analysis:
                bank_analysis[bank_name] = 0
            bank_analysis[bank_name] += 1
        
        return {
            'state_wise_data': state_analysis,
            'age_group_summary': age_group_analysis,
            'bank_summary': bank_analysis,
            'total_records': len(data),
            'processed_at': datetime.now().isoformat()
        }
    
    def save_to_json(self, data: Dict, output_file: str):
        """Save processed data to JSON file"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {output_file}")

def main():
    """Main function to process Excel files"""
    excel_folder = "../XLSx data"
    processor = ExcelDataProcessor(excel_folder)
    
    # Load pincode mapping
    processor.load_pincode_state_mapping()
    
    # Process Excel files
    processed_data = processor.process_excel_files()
    
    # Save results
    processor.save_to_json(processed_data, "processed_pensioner_data.json")
    
    # Print summary
    print("\n=== Processing Summary ===")
    print(f"Total records processed: {processed_data['total_records']}")
    print(f"States found: {len(processed_data['state_wise_data'])}")
    print(f"Age groups: {list(processed_data['age_group_summary'].keys())}")
    print(f"Banks: {len(processed_data['bank_summary'])}")
    
    return processed_data

if __name__ == "__main__":
    main()
