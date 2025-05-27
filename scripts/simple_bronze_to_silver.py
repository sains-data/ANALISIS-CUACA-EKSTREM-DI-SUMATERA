#!/usr/bin/env python3
"""
SIMPLE Bronze to Silver Processing - PASTI BISA!
Ambil Excel dari HDFS Bronze -> Process -> Upload CSV ke HDFS Silver
"""

import pandas as pd
import numpy as np
import os
import subprocess
import glob

def run_command(cmd):
    """Run command and return success status"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def main():
    print("🚀 SIMPLE Bronze to Silver Processing - DEADLINE FRIENDLY!")
    print("=" * 60)
      # Step 1: Download from HDFS Bronze
    print("📥 Step 1: Download Excel files from HDFS Bronze...")
    os.makedirs("temp_excel", exist_ok=True)
      # Download files from HDFS to container temp directory
    success, stdout, stderr = run_command("docker exec namenode mkdir -p /tmp/bronze_download")
    success, stdout, stderr = run_command("docker exec namenode hdfs dfs -get /data/bronze/* /tmp/bronze_download/")
    if not success:
        print(f"❌ Failed to download from HDFS: {stderr}")
        return
        
    # Copy files from container to host
    success2, stdout2, stderr2 = run_command("docker cp namenode:/tmp/bronze_download/ ./temp_excel/bronze/")
    if not success2:
        print(f"❌ Failed to copy from container: {stderr2}")
        return
    
    print("✅ Files downloaded from HDFS Bronze!")    # Step 2: Process Excel files
    print("\n📊 Step 2: Processing Excel files...")
    excel_files = glob.glob("temp_excel/bronze_download/*.xlsx")
    print(f"Found {len(excel_files)} Excel files")
    
    all_data = []
    
    for file_path in excel_files:
        try:
            print(f"📖 Processing: {os.path.basename(file_path)}")
            
            # Read Excel (skip header rows)
            df = pd.read_excel(file_path, skiprows=7)
            
            # Skip if empty
            if df.empty:
                continue
                
            # Clean column names
            df.columns = df.columns.str.strip()
            
            # Replace invalid values (8888, 9999, 0, -)
            numeric_cols = ['Tn', 'Tx', 'Tavg', 'RH_avg', 'RR', 'ss', 'ff_avg', 'ddd_x', 'ff_x']
            
            for col in numeric_cols:
                if col in df.columns:
                    # Replace invalid values
                    df[col] = df[col].replace([8888, 9999, 0, '-', ''], np.nan)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Simple interpolation
                    df[col] = df[col].interpolate(method='linear')
                    df[col] = df[col].fillna(method='ffill').fillna(method='bfill')
            
            # Remove empty rows
            df = df.dropna(how='all')
            
            if len(df) > 0:
                all_data.append(df)
                print(f"  ✅ Added {len(df)} rows")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            continue
    
    if not all_data:
        print("❌ No valid data found!")
        return
    
    # Step 3: Combine all data
    print(f"\n🔗 Step 3: Combining {len(all_data)} dataframes...")
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"✅ Combined data: {len(combined_df)} total rows")
    
    # Step 4: Save CSV
    print("\n💾 Step 4: Saving processed data...")
    csv_file = "processed_weather_data.csv"
    combined_df.to_csv(csv_file, index=False)
    print(f"✅ Saved CSV: {csv_file}")
    
    # Step 5: Upload to HDFS Silver
    print("\n📤 Step 5: Upload to HDFS Silver...")
    
    # Create Silver directory
    run_command("docker exec namenode hdfs dfs -mkdir -p /data/silver")
    
    # Upload CSV
    success, stdout, stderr = run_command(f"docker exec namenode hdfs dfs -put -f {csv_file} /data/silver/")
    if success:
        print("✅ Successfully uploaded to HDFS Silver!")
    else:
        print(f"❌ Failed to upload: {stderr}")
    
    # Step 6: Save to local Silver
    print("\n📁 Step 6: Save to local Silver...")
    os.makedirs("data/silver", exist_ok=True)
    local_silver = "data/silver/processed_weather_data.csv"
    combined_df.to_csv(local_silver, index=False)
    print(f"✅ Saved to local Silver: {local_silver}")
    
    # Step 7: Quality report
    print("\n📊 QUALITY REPORT")
    print("=" * 30)
    print(f"Total records: {len(combined_df):,}")
    print(f"Total columns: {len(combined_df.columns)}")
    
    numeric_cols = combined_df.select_dtypes(include=[np.number]).columns
    print(f"Numeric columns: {len(numeric_cols)}")
    
    for col in numeric_cols:
        missing = combined_df[col].isnull().sum()
        if missing > 0:
            pct = (missing / len(combined_df)) * 100
            print(f"  {col}: {missing} missing ({pct:.1f}%)")
    
    print("=" * 30)
    
    # Cleanup
    print("\n🧹 Cleanup...")
    try:
        import shutil
        shutil.rmtree("temp_excel")
        os.remove(csv_file)
        print("✅ Cleaned up temporary files")
    except:
        pass
    
    print("\n🎉 SUCCESS! Bronze → Silver processing completed!")
    print("📍 Results:")
    print("   - HDFS Silver: /data/silver/processed_weather_data.csv")
    print("   - Local Silver: data/silver/processed_weather_data.csv")
    print("=" * 60)

if __name__ == "__main__":
    main()
