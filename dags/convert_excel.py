import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_conversion.log'),
        logging.StreamHandler()
    ]
)

def handle_anomalies(df):
    """
    Handle anomalies in the dataframe:
    - Replace '-', '8888', '9999', '0' with NaN
    - Interpolate missing values
    """
    # List of numeric columns to process
    numeric_cols = ['TN', 'TX', 'TAVG', 'RH_AVG', 'RR', 'SS', 'FF_X', 'DDD_X', 'FF_AVG']
    
    for col in numeric_cols:
        # Convert to float, replacing anomalies with NaN
        df[col] = df[col].replace(['-', '8888', '9999', '0'], np.nan)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Interpolate missing values
        df[col] = df[col].interpolate(method='linear', limit_direction='both')
    
    return df

def clean_numeric_values(value):
    """Clean numeric values from the raw data (bronze layer)"""
    if pd.isna(value) or value == '-' or value == '8888' or value == '9999':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def convert_and_clean_excel(bronze_folder_path, silver_folder_path):
    """
    Convert Excel files from bronze layer (raw) to silver layer (cleaned)
    
    Args:
        bronze_folder_path: Path to the bronze layer containing raw Excel files
        silver_folder_path: Path to the silver layer for cleaned CSV output
    """
    # Create silver layer folder if it doesn't exist
    os.makedirs(silver_folder_path, exist_ok=True)
    
    successful_files = 0
    failed_files = 0
    
    # Read all files from bronze layer
    for file_name in os.listdir(bronze_folder_path):
        if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
            try:
                # Define Excel file path in bronze layer
                excel_file_path = os.path.join(bronze_folder_path, file_name)
                logging.info(f'Processing file from bronze layer: {file_name}')
                
                # Membaca file Excel
                excel_data = pd.ExcelFile(excel_file_path)
                
                # Ambil sheet pertama
                df = excel_data.parse(excel_data.sheet_names[0])
                
                # Membersihkan dan menetapkan nama kolom yang sesuai
                df_cleaned = df.iloc[5:].reset_index(drop=True)  # Mengambil data mulai dari baris ke-6
                df_cleaned.columns = ['TANGGAL', 'TN', 'TX', 'TAVG', 'RH_AVG', 'RR', 'SS', 'FF_X', 'DDD_X', 'FF_AVG', 'DDD_CAR']
                
                # Menghapus baris yang tidak diperlukan
                df_cleaned = df_cleaned[~df_cleaned['TANGGAL'].str.contains('TANGGAL|KETERANGAN|9999|8888|Tn:|Tx:|Tavg:|RH_avg:|RR:|ss:|ff_x:|ddd_x:|ff_avg:|ddd_car:', na=False)]
                df_cleaned = df_cleaned.dropna(how='all')
                df_cleaned = df_cleaned[~df_cleaned['TANGGAL'].isna()]
                
                # Handle anomalies and interpolate
                df_cleaned = handle_anomalies(df_cleaned)
                
                # Convert TANGGAL to datetime and format
                df_cleaned['TANGGAL'] = pd.to_datetime(df_cleaned['TANGGAL'], format='%d-%m-%Y', errors='coerce')
                df_cleaned['TANGGAL'] = df_cleaned['TANGGAL'].dt.strftime('%Y-%m-%d')
                
                # Remove any remaining invalid rows
                df_cleaned = df_cleaned.dropna(subset=['TANGGAL'])
                
                # Tentukan nama file output CSV
                output_file_name = file_name.replace('.xlsx', '.csv').replace('.xls', '.csv')
                output_file_name = output_file_name.replace(' ', '-')
                output_csv_file = os.path.join(silver_folder_path, output_file_name)
                
                # Simpan ke CSV
                df_cleaned.to_csv(output_csv_file, index=False)
                logging.info(f'Successfully converted {file_name} to {output_file_name}')
                successful_files += 1
                
            except Exception as e:
                logging.error(f'Error processing {file_name}: {str(e)}')
                failed_files += 1
    
    # Log summary
    logging.info(f'\nConversion Summary:')
    logging.info(f'Successfully converted: {successful_files} files')
    logging.info(f'Failed to convert: {failed_files} files')
    return successful_files, failed_files

if __name__ == "__main__":
    # Tentukan folder input dan output
    current_dir = os.path.dirname(os.path.abspath(__file__))
    excel_folder_path = os.path.join(current_dir, 'data', 'raw')
    output_folder_path = os.path.join(current_dir, 'data', 'processed')
    
    print(f"Membaca file Excel dari: {excel_folder_path}")
    print(f"Menyimpan CSV ke: {output_folder_path}")
    
    # Jalankan konversi
    successful, failed = convert_and_clean_excel(excel_folder_path, output_folder_path)
