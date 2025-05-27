#!/usr/bin/env python3
"""
HDFS Bronze to Silver Processing - DIRECT FROM HDFS
Langsung ambil Excel dari HDFS Bronze -> Process -> Save ke HDFS Silver
TANPA FOLDER TEMPORARY!
"""

import subprocess
import pandas as pd
import os
from datetime import datetime

def run_hdfs_command(command):
    """Run HDFS command via namenode container"""
    try:
        full_command = f"docker exec namenode {command}"
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def process_excel_in_container():
    """Process Excel files directly in namenode container"""
    
    # Create processing script inside container
    processing_script = '''
import pandas as pd
import os
import glob

print("🚀 Processing Excel files directly from HDFS...")

# Download all Excel files from HDFS to container temp
os.system("hdfs dfs -get /data/bronze/*.xlsx /tmp/")

# Find all Excel files in temp
excel_files = glob.glob("/tmp/*.xlsx")
print(f"Found {len(excel_files)} Excel files")

all_data = []

for file_path in excel_files:
    try:
        print(f"📖 Processing: {file_path}")
        
        # Read Excel file
        df = pd.read_excel(file_path)
        
        # Basic cleaning - remove null rows
        df = df.dropna(how='all')
        
        # Convert problematic values to NaN
        numeric_cols = ['Tn', 'Tx', 'Tavg', 'RH_avg', 'RR', 'ss', 'ff_avg', 'ddd_x', 'ff_x']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if len(df) > 0:
            all_data.append(df)
            print(f"  ✅ Added {len(df)} rows")
            
    except Exception as e:
        print(f"  ❌ Error processing {file_path}: {e}")
        continue

if all_data:
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"✅ Combined data: {len(combined_df)} total rows")
    
    # Save as CSV
    csv_file = "/tmp/weather_processed.csv"
    combined_df.to_csv(csv_file, index=False)
    print(f"💾 Saved CSV: {csv_file}")
    
    # Upload to HDFS Silver
    os.system("hdfs dfs -mkdir -p /data/silver")
    os.system(f"hdfs dfs -put -f {csv_file} /data/silver/weather_data_processed.csv")
    
    print("✅ Successfully uploaded to HDFS Silver!")
    print("📊 Data Summary:")
    print(f"   Total Records: {len(combined_df)}")
    print(f"   Columns: {list(combined_df.columns)}")
    
    # Show first few rows
    print("\\n🔍 Sample Data:")
    print(combined_df.head())
    
else:
    print("❌ No data to process!")
'''
    
    return processing_script

def main():
    print("🚀 HDFS Bronze to Silver Processing - DIRECT METHOD!")
    print("=" * 60)
    
    # Step 1: Check HDFS Bronze layer
    print("📥 Step 1: Checking HDFS Bronze layer...")
    success, stdout, stderr = run_hdfs_command("hdfs dfs -ls /data/bronze/")
    
    if not success:
        print(f"❌ HDFS Bronze not accessible: {stderr}")
        return
    
    # Count files
    file_count = len([line for line in stdout.split('\n') if '.xlsx' in line])
    print(f"✅ Found {file_count} Excel files in HDFS Bronze")
    
    # Step 2: Install pandas in container if needed
    print("\n🔧 Step 2: Preparing container environment...")
    install_cmd = "pip install pandas openpyxl xlrd"
    success, stdout, stderr = run_hdfs_command(install_cmd)
    
    if success:
        print("✅ Pandas installed in container")
    else:
        print("⚠️ Pandas installation skipped (might already exist)")
    
    # Step 3: Create processing script in container
    print("\n📝 Step 3: Creating processing script in container...")
    script_content = process_excel_in_container()
    
    # Write script to container
    script_creation = f'''cat > /tmp/process_excel.py << 'EOF'
{script_content}
EOF'''
    
    success, stdout, stderr = run_hdfs_command(script_creation)
    
    if not success:
        print(f"❌ Failed to create script: {stderr}")
        return
    
    print("✅ Processing script created in container")
    
    # Step 4: Execute processing script
    print("\n🚀 Step 4: Executing Bronze → Silver processing...")
    success, stdout, stderr = run_hdfs_command("python3 /tmp/process_excel.py")
    
    if success:
        print("✅ Processing completed successfully!")
        print("\n📋 Processing Output:")
        print(stdout)
    else:
        print(f"❌ Processing failed: {stderr}")
        print(f"📋 Output: {stdout}")
        return
    
    # Step 5: Verify Silver layer
    print("\n🔍 Step 5: Verifying HDFS Silver layer...")
    success, stdout, stderr = run_hdfs_command("hdfs dfs -ls /data/silver/")
    
    if success:
        print("✅ HDFS Silver layer verified:")
        print(stdout)
    else:
        print(f"❌ Silver layer verification failed: {stderr}")
    
    # Step 6: Download result to local for verification
    print("\n📤 Step 6: Downloading result to local...")
    
    # Create local silver directory
    os.makedirs("data/silver", exist_ok=True)
    
    # Download from HDFS to container then to local
    run_hdfs_command("hdfs dfs -get /data/silver/weather_data_processed.csv /tmp/")
    
    # Copy from container to local
    local_cmd = "docker cp namenode:/tmp/weather_data_processed.csv ./data/silver/"
    result = subprocess.run(local_cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ Result downloaded to local: data/silver/weather_data_processed.csv")
        
        # Show local file info
        local_file = "data/silver/weather_data_processed.csv"
        if os.path.exists(local_file):
            df = pd.read_csv(local_file)
            print(f"\n📊 Local File Summary:")
            print(f"   Records: {len(df)}")
            print(f"   Columns: {list(df.columns)}")
            print(f"   File Size: {os.path.getsize(local_file)} bytes")
    else:
        print(f"❌ Failed to download to local: {result.stderr}")
    
    # Cleanup
    print("\n🧹 Cleanup...")
    run_hdfs_command("rm -f /tmp/*.xlsx /tmp/weather_processed.csv /tmp/process_excel.py")
    
    print("\n🎉 Bronze → Silver processing completed!")
    print("📁 Results available at:")
    print("   - HDFS: /data/silver/weather_data_processed.csv")
    print("   - Local: data/silver/weather_data_processed.csv")

if __name__ == "__main__":
    main()
