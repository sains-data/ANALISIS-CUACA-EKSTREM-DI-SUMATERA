#!/usr/bin/env python3
"""
FINAL Bronze to Silver Processing - SIMPLE & GUARANTEED TO WORK!
Mengambil Excel dari HDFS Bronze -> Membersihkan data -> Upload CSV ke HDFS Silver
NO INTERPOLATION - Just clean and combine data
"""

import pandas as pd
import numpy as np
import os
import subprocess
import glob
import shutil
from datetime import datetime

def run_command(cmd):
    """Run shell command and return success status"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def clean_data_simple(df):
    """Clean data by replacing invalid values with NaN"""
    print("🧹 Cleaning invalid values (8888, 9999, -, 0)...")
    
    # Columns that should be numeric
    numeric_cols = ['Tn', 'Tx', 'Tavg', 'RH_avg', 'RR', 'ss', 'ff_avg', 'ddd_x', 'ff_x']
    
    for col in numeric_cols:
        if col in df.columns:
            # Replace invalid values
            df[col] = df[col].replace([8888, 9999, 0, '-', '', ' '], np.nan)
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def main():
    print("🚀 FINAL Bronze to Silver Processing - SIMPLE VERSION!")
    print("=" * 60)
    
    # Create temp directory
    temp_dir = "temp_processing"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Step 1: Download from HDFS Bronze
        print("\n📥 Step 1: Download Excel files from HDFS Bronze...")
        
        # Create temp directory in container
        success, stdout, stderr = run_command("docker exec namenode mkdir -p /tmp/bronze_files")
        
        # Download files from HDFS to container
        success, stdout, stderr = run_command("docker exec namenode hdfs dfs -get /data/bronze/*.xlsx /tmp/bronze_files/")
        if not success:
            print(f"❌ Failed to download from HDFS: {stderr}")
            return
        
        # Copy files from container to host
        success, stdout, stderr = run_command(f"docker cp namenode:/tmp/bronze_files/ {temp_dir}/")
        if not success:
            print(f"❌ Failed to copy from container: {stderr}")
            return
        
        print("✅ Files downloaded successfully!")
        
        # Step 2: Process Excel files
        print("\n📊 Step 2: Processing Excel files...")
        excel_files = glob.glob(f"{temp_dir}/bronze_files/*.xlsx")
        print(f"Found {len(excel_files)} Excel files")
        
        if len(excel_files) == 0:
            print("❌ No Excel files found!")
            return
        
        all_data = []
        processed_count = 0
        
        for file_path in excel_files:
            try:
                filename = os.path.basename(file_path)
                print(f"📖 Processing: {filename}")
                
                # Read Excel (skip header rows if needed)
                df = pd.read_excel(file_path, skiprows=7)
                
                if df.empty:
                    print(f"  ⚠️ Empty file: {filename}")
                    continue
                
                # Clean data
                df_clean = clean_data_simple(df)
                
                # Remove completely empty rows
                df_clean = df_clean.dropna(how='all')
                
                if len(df_clean) > 0:
                    # Add source file column
                    df_clean['source_file'] = filename
                    all_data.append(df_clean)
                    processed_count += 1
                    print(f"  ✅ Added {len(df_clean)} rows from {filename}")
                else:
                    print(f"  ⚠️ No valid data in: {filename}")
                    
            except Exception as e:
                print(f"  ❌ Error processing {filename}: {e}")
                continue
        
        if not all_data:
            print("❌ No valid data found in any files!")
            return
        
        # Step 3: Combine all data
        print(f"\n🔗 Step 3: Combining data from {processed_count} files...")
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"✅ Combined data: {len(combined_df)} total rows")
        
        # Step 4: Basic statistics
        print("\n📈 Step 4: Data Statistics...")
        print(f"Total rows: {len(combined_df)}")
        print(f"Total columns: {len(combined_df.columns)}")
        
        # Show missing data count
        missing_data = combined_df.isnull().sum()
        print("\nMissing data per column:")
        for col, count in missing_data.items():
            if count > 0:
                percentage = (count / len(combined_df)) * 100
                print(f"  {col}: {count} ({percentage:.1f}%)")
        
        # Step 5: Save to CSV
        print("\n💾 Step 5: Saving processed data...")
        csv_filename = "weather_data_processed_final.csv"
        combined_df.to_csv(csv_filename, index=False)
        print(f"✅ Saved CSV: {csv_filename}")
        
        # Step 6: Upload to HDFS Silver
        print("\n📤 Step 6: Upload to HDFS Silver...")
        
        # Create Silver directory in HDFS
        run_command("docker exec namenode hdfs dfs -mkdir -p /data/silver")
        
        # Copy CSV to container
        success, stdout, stderr = run_command(f"docker cp {csv_filename} namenode:/tmp/")
        if not success:
            print(f"❌ Failed to copy CSV to container: {stderr}")
            return
        
        # Upload to HDFS
        success, stdout, stderr = run_command(f"docker exec namenode hdfs dfs -put -f /tmp/{csv_filename} /data/silver/")
        if success:
            print("✅ Successfully uploaded to HDFS Silver!")
            print(f"📁 HDFS location: hdfs://namenode:9000/data/silver/{csv_filename}")
        else:
            print(f"❌ Failed to upload to HDFS: {stderr}")
        
        # Step 7: Also save to local Silver directory
        print("\n📁 Step 7: Save to local Silver directory...")
        os.makedirs("data/silver", exist_ok=True)
        local_silver_file = f"data/silver/{csv_filename}"
        combined_df.to_csv(local_silver_file, index=False)
        print(f"✅ Local copy saved: {local_silver_file}")
        
        # Step 8: Verify HDFS upload
        print("\n✅ Step 8: Verification...")
        success, stdout, stderr = run_command("docker exec namenode hdfs dfs -ls /data/silver/")
        if success:
            print("HDFS Silver layer contents:")
            print(stdout)
        
        print("\n🎉 SUCCESS! Bronze → Silver processing completed!")
        print("=" * 60)
        print(f"📊 Processed {processed_count} Excel files")
        print(f"📈 Total records: {len(combined_df)}")
        print(f"📁 HDFS Silver: /data/silver/{csv_filename}")
        print(f"📁 Local Silver: {local_silver_file}")
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup temp directory
        print("\n🧹 Cleaning up temporary files...")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(csv_filename):
            os.remove(csv_filename)
        
        # Cleanup container temp files
        run_command("docker exec namenode rm -rf /tmp/bronze_files")
        run_command(f"docker exec namenode rm -f /tmp/{csv_filename}")

if __name__ == "__main__":
    main()
