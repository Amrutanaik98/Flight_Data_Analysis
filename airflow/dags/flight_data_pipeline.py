from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess
import boto3
import logging
import os
import time

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

AWS_REGION = "us-east-1"
S3_BUCKET = "flights-data-lake-amruta"
SQS_QUEUE_NAME = "flight_dev"
GLUE_JOB_NAME = "flights-glue-job-dev"

# File paths
PROJECT_ROOT = "/home/ubuntu/Flight_Data_Analysis"
PRODUCER_SCRIPT = f"{PROJECT_ROOT}/src/producer.py"
ANALYTICS_SCRIPT = f"{PROJECT_ROOT}/analytics/flight_analytics.py"
REPORTS_SCRIPT = f"{PROJECT_ROOT}/analytics/generate_reports.py"
TRAIN_ML_SCRIPT = f"{PROJECT_ROOT}/machine_learning/train_model.py"
PREDICT_ML_SCRIPT = f"{PROJECT_ROOT}/machine_learning/predict.py"

# ============================================================
# DAG CONFIGURATION
# ============================================================

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'flight_data_ml_pipeline',
    default_args=default_args,
    description='Flight Data Pipeline: Producer → S3 raw → Glue → S3 processed → ML',
    schedule_interval='@daily',
    catchup=False,
    tags=['flights', 'production', 'data-pipeline', 'analytics', 'ml'],
)

# ============================================================
# HELPER FUNCTION: Run Python Script
# ============================================================

def run_python_script(script_path, step_name, timeout=900):
    """Execute Python script with error handling"""
    logger.info("=" * 70)
    logger.info(f"🚀 {step_name}")
    logger.info("=" * 70)
    logger.info(f"Script: {script_path}")
    logger.info("")
    
    if not os.path.exists(script_path):
        error_msg = f"❌ Script not found: {script_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        result = subprocess.run(
            ['python3', script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=PROJECT_ROOT,
            env={**os.environ, 'PYTHONUNBUFFERED': '1'}
        )
        
        if result.stdout:
            logger.info(f"OUTPUT:\n{result.stdout}")
        
        if result.returncode != 0:
            error_output = result.stderr if result.stderr else "Unknown error"
            logger.error(f"STDERR:\n{error_output}")
            raise Exception(f"{step_name} failed with exit code {result.returncode}:\n{error_output}")
        
        logger.info(f"✅ {step_name} completed successfully")
        print("=" * 70)
        return f"{step_name} executed successfully"
        
    except subprocess.TimeoutExpired:
        error_msg = f"⏱️ {step_name} timed out after {timeout}s"
        logger.error(error_msg)
        raise TimeoutError(error_msg)
    except Exception as e:
        logger.error(f"❌ {step_name} failed: {str(e)}")
        raise

# ============================================================
# PHASE 1: PRODUCER
# ============================================================

def run_producer_script():
    """Fetch flight data from API and save to S3"""
    logger.info("=" * 70)
    logger.info("📥 PHASE 1: PRODUCER")
    logger.info("=" * 70)
    logger.info("Fetching flight API data → S3 raw/ + SQS")
    logger.info("")
    
    return run_python_script(PRODUCER_SCRIPT, "Producer Script", timeout=600)

# ============================================================
# PHASE 2: VERIFY RAW DATA
# ============================================================

def verify_raw_data_in_s3():
    """Verify raw data exists in S3"""
    logger.info("=" * 70)
    logger.info("📂 PHASE 2: VERIFY RAW DATA")
    logger.info("=" * 70)
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix='raw/', MaxKeys=100)
        
        if 'Contents' not in response:
            logger.warning("⚠️ No raw data in S3")
            return "No raw data"
        
        file_count = len(response['Contents'])
        logger.info(f"✅ Found {file_count} files in S3 raw/")
        logger.info("")
        return f"Raw data: {file_count} files"
        
    except Exception as e:
        logger.warning(f"⚠️ Raw data check failed: {str(e)}")
        return "Raw data check failed"

# ============================================================
# PHASE 3: TRIGGER GLUE JOB
# ============================================================

def trigger_glue_job_func():
    """Trigger Glue job to process data"""
    logger.info("=" * 70)
    logger.info("⚙️  PHASE 3: TRIGGER GLUE JOB")
    logger.info("=" * 70)
    
    try:
        # ✅ EXPLICIT: Create boto3 client with explicit configuration
        import botocore.session
        
        # Get AWS credentials from environment
        logger.info("Attempting to trigger Glue job...")
        logger.info(f"Job Name: {GLUE_JOB_NAME}")
        logger.info(f"Region: {AWS_REGION}")
        logger.info("")
        
        # Create Glue client
        glue_client = boto3.client('glue', region_name=AWS_REGION)
        
        # Verify credentials
        try:
            sts = boto3.client('sts', region_name=AWS_REGION)
            identity = sts.get_caller_identity()
            logger.info(f"✅ Using AWS Account: {identity['Account']}")
            logger.info(f"   ARN: {identity['Arn']}")
        except Exception as cred_error:
            logger.warning(f"⚠️ Could not verify credentials: {cred_error}")
        
        logger.info("")
        logger.info(f"Input: s3://{S3_BUCKET}/raw/")
        logger.info(f"Output: s3://{S3_BUCKET}/processed/")
        logger.info("")
        
        # Start job run with explicit parameters
        logger.info("Calling glue_client.start_job_run()...")
        
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--S3_INPUT_PATH': f's3://{S3_BUCKET}/raw/',
                '--S3_OUTPUT_PATH': f's3://{S3_BUCKET}/processed/',
            }
        )
        
        job_run_id = response['JobRunId']
        logger.info(f"✅ SUCCESS! Glue job triggered")
        logger.info(f"   Job Run ID: {job_run_id}")
        logger.info("")
        return f"Glue triggered: {job_run_id}"
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error("❌ GLUE JOB TRIGGER FAILED")
        logger.error("=" * 70)
        logger.error(f"Exception Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.error("")
        logger.error("TROUBLESHOOTING:")
        logger.error("1. Verify Glue job name is correct")
        logger.error("2. Check EC2 IAM role has Glue permissions")
        logger.error("3. Verify AWS credentials are configured")
        logger.error("4. Check Glue job script location in AWS console")
        logger.error("")
        import traceback
        logger.error(f"Full Traceback:\n{traceback.format_exc()}")
        raise

# ============================================================
# PHASE 4: WAIT FOR GLUE (2 MINUTES)
# ============================================================

def wait_for_glue_processing():
    """Wait 2 minutes for Glue to process"""
    logger.info("=" * 70)
    logger.info("⏳ PHASE 4: WAIT FOR GLUE (2 MINUTES)")
    logger.info("=" * 70)
    logger.info("Waiting for Glue job to complete...")
    logger.info("")
    
    for i in range(12):
        remaining = (12 - i) * 10
        logger.info(f"  ⏳ {remaining} seconds remaining...")
        time.sleep(10)
    
    logger.info("")
    logger.info("✅ Glue processing complete!")
    logger.info("")
    
    return "Glue processing completed"

# ============================================================
# PHASE 5: VERIFY PROCESSED DATA
# ============================================================

def verify_processed_data():
    """Verify processed data in S3"""
    logger.info("=" * 70)
    logger.info("📊 PHASE 5: VERIFY PROCESSED DATA")
    logger.info("=" * 70)
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix='processed/')
        
        if 'Contents' not in response:
            logger.warning("⚠️ No processed data found")
            return "No processed data"
        
        file_count = len(response['Contents'])
        logger.info(f"✅ Found {file_count} objects in S3 processed/")
        
        # Check for expected folders
        expected = ['flights_main', 'flights_by_airline', 'flights_by_route', 'flights_by_status']
        found_folders = set()
        
        for obj in response['Contents']:
            for folder in expected:
                if folder in obj['Key']:
                    found_folders.add(folder)
        
        logger.info("📁 Folders:")
        for folder in expected:
            status = "✅" if folder in found_folders else "❌"
            logger.info(f"  {status} {folder}/")
        
        logger.info("")
        return f"Processed data verified"
        
    except Exception as e:
        logger.warning(f"⚠️ Verification failed: {str(e)}")
        return "Verification failed"

# ============================================================
# PHASE 6: ANALYTICS
# ============================================================

def run_analytics():
    """Run analytics engine"""
    logger.info("=" * 70)
    logger.info("📊 PHASE 6: ANALYTICS")
    logger.info("=" * 70)
    logger.info("")
    
    return run_python_script(ANALYTICS_SCRIPT, "Analytics Engine", timeout=600)

# ============================================================
# PHASE 7: ML TRAINING
# ============================================================

def train_ml_model():
    """Train ML model"""
    logger.info("=" * 70)
    logger.info("🤖 PHASE 7: ML TRAINING")
    logger.info("=" * 70)
    logger.info("")
    
    return run_python_script(TRAIN_ML_SCRIPT, "ML Model Training", timeout=900)

# ============================================================
# PHASE 8: ML PREDICTIONS
# ============================================================

def make_predictions():
    """Make predictions"""
    logger.info("=" * 70)
    logger.info("⚡ PHASE 8: ML PREDICTIONS")
    logger.info("=" * 70)
    logger.info("")
    
    return run_python_script(PREDICT_ML_SCRIPT, "ML Predictions", timeout=600)

# ============================================================
# PHASE 9: GENERATE REPORTS
# ============================================================

def run_report_generator():
    """Generate final reports"""
    logger.info("=" * 70)
    logger.info("📧 PHASE 9: REPORTS")
    logger.info("=" * 70)
    logger.info("")
    
    return run_python_script(REPORTS_SCRIPT, "Report Generator", timeout=600)

# ============================================================
# COMPLETION
# ============================================================

def send_completion_summary():
    """Send completion notification"""
    logger.info("=" * 70)
    logger.info("✅ PIPELINE COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    logger.info("🎉 Flight Data ML Pipeline Completed Successfully!")
    logger.info("")
    logger.info("Flow Summary:")
    logger.info("  1. Producer → S3 raw/ + SQS")
    logger.info("  2. Lambda (auto) → processes SQS")
    logger.info("  3. Glue Job → cleans raw/ data")
    logger.info("  4. Wait 2 min → Glue completes")
    logger.info("  5. Verify → processed/ folders created")
    logger.info("  6. Analytics → analyze data")
    logger.info("  7. ML Training → train model")
    logger.info("  8. Predictions → predict delays")
    logger.info("  9. Reports → generate final reports")
    logger.info("")
    
    return "Pipeline complete!"

# ============================================================
# DEFINE TASKS
# ============================================================

task_start = BashOperator(
    task_id='start',
    bash_command='echo "🚀 Pipeline Started"',
    dag=dag,
)

task_producer = PythonOperator(
    task_id='step_1_producer',
    python_callable=run_producer_script,
    dag=dag,
)

task_verify_raw = PythonOperator(
    task_id='step_2_verify_raw',
    python_callable=verify_raw_data_in_s3,
    dag=dag,
)

task_trigger_glue = PythonOperator(
    task_id='step_3_trigger_glue',
    python_callable=trigger_glue_job_func,
    dag=dag,
)

task_wait_glue = PythonOperator(
    task_id='step_4_wait_glue',
    python_callable=wait_for_glue_processing,
    dag=dag,
)

task_verify_processed = PythonOperator(
    task_id='step_5_verify_processed',
    python_callable=verify_processed_data,
    dag=dag,
)

task_analytics = PythonOperator(
    task_id='step_6_analytics',
    python_callable=run_analytics,
    dag=dag,
)

task_train_ml = PythonOperator(
    task_id='step_7_train_ml',
    python_callable=train_ml_model,
    dag=dag,
)

task_predict = PythonOperator(
    task_id='step_8_predict',
    python_callable=make_predictions,
    dag=dag,
)

task_reports = PythonOperator(
    task_id='step_9_reports',
    python_callable=run_report_generator,
    dag=dag,
)

task_complete = PythonOperator(
    task_id='step_10_complete',
    python_callable=send_completion_summary,
    dag=dag,
)

task_end = BashOperator(
    task_id='end',
    bash_command='echo "✅ Pipeline Complete!"',
    dag=dag,
)

# ============================================================
# DEFINE TASK DEPENDENCIES
# ============================================================

task_start >> task_producer >> task_verify_raw >> task_trigger_glue >> task_wait_glue >> task_verify_processed >> task_analytics >> task_train_ml >> task_predict >> task_reports >> task_complete >> task_end
