from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add plugins folder to path to import custom modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../plugins'))

from github_extractor import GitHubExtractor
from compliance_transformer import ComplianceTransformer

# --- Configuration ---
REPO_OWNER = "home-assistant"
REPO_NAME = "core"
DATA_DIR = "/opt/airflow/data"
RAW_FILENAME = "daily_prs.json"

def run_extraction(**kwargs):
    """
    Task 1: Extract data from GitHub API.
    Fetches PRs, Reviews, and Status Checks.
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN environment variable is missing.")

    print(f"Starting extraction for {REPO_OWNER}/{REPO_NAME}...")
    extractor = GitHubExtractor(REPO_OWNER, REPO_NAME, token)
    
    # Fetch data (limit set to 50 for assignment scope)
    enriched_data = extractor.fetch_full_data(limit=50)
    
    # Save raw data
    extractor.save_to_json(enriched_data, RAW_FILENAME)
    print(f"Extraction complete. Raw data saved.")

def run_transformation(**kwargs):
    """
    Task 2: Transform raw data.
    Applies compliance logic (Reviews + Checks) and saves as Parquet.
    """
    input_path = os.path.join(DATA_DIR, RAW_FILENAME)
    
    # Generate output filename with execution date (YYYYMMDD)
    execution_date = kwargs['ds_nodash']
    output_filename = f"compliance_report_{execution_date}.parquet"
    output_path = os.path.join(DATA_DIR, output_filename)

    print(f"Transforming {input_path} -> {output_path}")
    transformer = ComplianceTransformer(input_path, output_path)
    transformer.transform()

def run_loading(**kwargs):
    """
    Task 3: Load / Verification.
    Verifies that the Parquet file was created successfully.
    Simulates the 'Load' step of ETL.
    """
    execution_date = kwargs['ds_nodash']
    expected_file = os.path.join(DATA_DIR, f"compliance_report_{execution_date}.parquet")
    
    if os.path.exists(expected_file):
        print(f"SUCCESS: Data successfully loaded to {expected_file}")
    else:
        raise FileNotFoundError(f"FAILED: Output file {expected_file} was not found.")

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'scytale_compliance_etl',
    default_args=default_args,
    description='ETL pipeline for GitHub Code Review Compliance',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scytale', 'etl'],
) as dag:

    t1_extract = PythonOperator(
        task_id='extract_task',
        python_callable=run_extraction,
        provide_context=True,
    )

    t2_transform = PythonOperator(
        task_id='transform_task',
        python_callable=run_transformation,
        provide_context=True,
    )

    t3_load = PythonOperator(
        task_id='load_task',
        python_callable=run_loading,
        provide_context=True,
    )

    # Define dependencies
    t1_extract >> t2_transform >> t3_load