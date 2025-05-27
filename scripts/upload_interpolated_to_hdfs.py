#!/usr/bin/env python3
"""
Upload Interpolated Data to HDFS
Mengupload data yang sudah diinterpolasi ke HDFS Silver layer
"""

import os
import subprocess
import pandas as pd
from datetime import datetime

def check_hdfs_connection():
    """
    Memeriksa koneksi ke HDFS
    """
    try:
        # Test HDFS connection using docker exec
        result = subprocess.run([
            'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/'
        ], capture_output=True, text=True, check=True)
        print("✅ HDFS connection successful")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ HDFS connection failed: {e}")
        return False

def upload_to_hdfs(local_file, hdfs_path):
    """
    Upload file ke HDFS
    """
    try:
        # Buat direktori HDFS jika belum ada
        subprocess.run([
            'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', 
            os.path.dirname(hdfs_path)
        ], check=True)
        
        # Copy file dari container ke namenode
        subprocess.run([
            'docker', 'cp', local_file, f'namenode:/tmp/{os.path.basename(local_file)}'
        ], check=True)
        
        # Upload ke HDFS
        subprocess.run([
            'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', 
            f'/tmp/{os.path.basename(local_file)}', hdfs_path
        ], check=True)
        
        # Cleanup temporary file
        subprocess.run([
            'docker', 'exec', 'namenode', 'rm', f'/tmp/{os.path.basename(local_file)}'
        ], check=True)
        
        print(f"✅ File uploaded to HDFS: {hdfs_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to upload {local_file} to HDFS: {e}")
        return False

def main():
    print("🔄 UPLOADING INTERPOLATED DATA TO HDFS")
    print("="*50)
    
    # Check HDFS connection
    if not check_hdfs_connection():
        print("❌ Cannot proceed without HDFS connection")
        return
    
    # File paths
    silver_local_path = "d:/Coding/Python/src/ABD/project-bigdata/data/silver"
    interpolated_file = os.path.join(silver_local_path, "weather_data_interpolated.csv")
    summary_file = os.path.join(silver_local_path, "data_quality_summary.csv")
    
    # Check if files exist
    if not os.path.exists(interpolated_file):
        print(f"❌ File not found: {interpolated_file}")
        print("Please run the interpolation script first!")
        return
    
    # Upload files to HDFS
    hdfs_silver_path = "/data/silver"
    
    # Upload interpolated data
    if upload_to_hdfs(interpolated_file, f"{hdfs_silver_path}/weather_data_interpolated.csv"):
        print(f"📊 Interpolated data uploaded to HDFS Silver layer")
    
    # Upload summary if exists
    if os.path.exists(summary_file):
        if upload_to_hdfs(summary_file, f"{hdfs_silver_path}/data_quality_summary.csv"):
            print(f"📈 Quality summary uploaded to HDFS Silver layer")
    
    # Verify upload
    try:
        result = subprocess.run([
            'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', hdfs_silver_path
        ], capture_output=True, text=True, check=True)
        
        print(f"\n📁 Files in HDFS Silver layer:")
        print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to list HDFS directory: {e}")
    
    print("\n✅ HDFS UPLOAD COMPLETE!")

if __name__ == "__main__":
    main()
