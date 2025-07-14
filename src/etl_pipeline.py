import os
import logging
from extract import extract_csv
from transform import transform_data
from load import load_to_postgres

# Get absolute log file path
log_file_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'etl_log.txt')

# Setup logging with timestamp and formatting
logging.basicConfig(
    filename=os.path.abspath(log_file_path),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_etl():
    logging.info("ETL process started.")

    # Adjusted paths for running from project root
    files_tables = {
        "data/students.csv": "students",
        "data/staff.csv": "staff",
        "data/finance.csv": "finance"
    }

    for file_path, table_name in files_tables.items():
        logging.info(f"Processing file: {file_path} for table: {table_name}")
        try:
            df = extract_csv(file_path)
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
            continue

        if not df.empty:
            logging.info(f"Data extracted from {file_path} with {len(df)} records.")
            transformed = transform_data(df)
            logging.info(f"Data transformed for table: {table_name}")
            load_to_postgres(transformed, table_name)
            logging.info(f"Data loaded into table: {table_name}")
        else:
            logging.warning(f"No data found in {file_path}, skipping...")

    logging.info("ETL process completed.")

if __name__ == "__main__":
    run_etl()
    print("ETL process completed. Check logs/etl_log.txt for details.")
