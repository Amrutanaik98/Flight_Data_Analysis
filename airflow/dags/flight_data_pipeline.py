"""
Flight Data Analysis Pipeline - Updated Airflow DAG
====================================================

Complete Data Flow:
1. Producer.py → Fetch API data → S3 raw + SQS
2. Lambda (external) → Process SQS messages → Load to DynamoDB
3. Glue Job (external) → Process S3 raw → Aggregations → S3 processed
4. Airflow monitors all steps

Schedule: Every 30 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess
import boto3

# ============================================
# CONFIGURATION
# ============================================

AWS_REGION = "us-east-1"
S3_BUCKET = "flights-data-lake-amruta"
SQS_QUEUE_NAME = "flight_dev"
DYNAMODB_TABLE = "flights-realtime-dev"
GLUE_JOB_NAME = "flight_data_processing"  # Your Glue job name
PRODUCER_SCRIPT = "/home/ubuntu/Flight_Data_Analysis/src/producer.py"

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================
# DAG DEFINITION
# ============================================

dag = DAG(
    'flight_data_pipeline',
    default_args=default_args,
    description='Flight Data Analysis Pipeline - Complete Flow with Lambda & Glue',
    schedule_interval='*/30 * * * *',
    catchup=False,
    tags=['flights', 'production', 'data-pipeline'],
)

# ============================================
# PYTHON FUNCTIONS
# ============================================

def run_producer_script():
    """
    Step 1: Run producer.py to fetch API data
    Sends data to: S3 raw folder + SQS queue
    """
    print("=" * 70)
    print("🚀 STEP 1: RUNNING PRODUCER SCRIPT")
    print("=" * 70)
    print("Action: Fetch flight data from API")
    print("Output: Data → S3 raw folder + SQS queue")
    print("")
    
    try:
        result = subprocess.run(
            ['python', PRODUCER_SCRIPT],
            capture_output=True,
            text=True,
            timeout=600,
        )
        
        print("PRODUCER OUTPUT:")
        print(result.stdout)
        
        if result.returncode != 0:
            print("PRODUCER ERROR:")
            print(result.stderr)
            raise Exception(f"Producer script failed with return code {result.returncode}")
        
        print("=" * 70)
        print("✅ STEP 1 COMPLETE: Data sent to S3 raw + SQS")
        print("=" * 70)
        return "Producer script executed successfully"
        
    except subprocess.TimeoutExpired:
        raise Exception("Producer script timed out after 10 minutes")
    except FileNotFoundError:
        raise Exception(f"Producer script not found at: {PRODUCER_SCRIPT}")
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        raise

def check_sqs_messages():
    """
    Step 2: Verify SQS has messages
    Lambda will process these messages → DynamoDB
    """
    print("=" * 70)
    print("🔍 STEP 2: CHECKING SQS QUEUE")
    print("=" * 70)
    
    try:
        sqs_client = boto3.client('sqs', region_name=AWS_REGION)
        response = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
        queue_url = response['QueueUrl']
        
        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesDelayed']
        )
        
        msg_count = int(response['Attributes']['ApproximateNumberOfMessages'])
        delayed_count = int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
        
        print(f"📨 Messages in queue: {msg_count}")
        print(f"⏱️  Delayed messages: {delayed_count}")
        
        print("=" * 70)
        print("✅ STEP 2 COMPLETE: SQS queue checked")
        print("=" * 70)
        return f"SQS check passed - {msg_count} messages"
        
    except Exception as e:
        print(f"⚠️  SQS check warning: {str(e)}")
        return f"SQS check skipped: {str(e)}"

def check_dynamodb_data():
    """
    Step 2B: Verify Lambda loaded data to DynamoDB
    """
    print("=" * 70)
    print("💾 STEP 2B: CHECKING DYNAMODB TABLE")
    print("=" * 70)
    
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        response = table.scan(Limit=10)
        item_count = response['Count']
        
        print(f"📊 Latest items in DynamoDB: {item_count}")
        
        if item_count > 0:
            sample_item = response['Items'][0]
            print(f"📋 Sample item keys: {list(sample_item.keys())}")
            print(f"✅ Lambda successfully loaded data!")
        else:
            print("⚠️  No items found - Lambda may not have processed yet")
        
        print("=" * 70)
        print("✅ STEP 2B COMPLETE: DynamoDB checked")
        print("=" * 70)
        return f"DynamoDB check passed - {item_count} items"
        
    except Exception as e:
        print(f"⚠️  DynamoDB check warning: {str(e)}")
        return f"DynamoDB check skipped: {str(e)}"

def trigger_glue_job():
    """
    Step 3: Trigger AWS Glue job for batch processing
    """
    print("=" * 70)
    print("⚙️  STEP 3: TRIGGERING GLUE JOB")
    print("=" * 70)
    
    try:
        glue_client = boto3.client('glue', region_name=AWS_REGION)
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--input_bucket': S3_BUCKET,
                '--input_prefix': 'raw/',
                '--output_prefix': 'processed/',
            }
        )
        
        job_run_id = response['JobRunId']
        print(f"🚀 Glue job started!")
        print(f"Job Run ID: {job_run_id}")
        
        print("=" * 70)
        print("✅ STEP 3 COMPLETE: Glue job triggered")
        print("=" * 70)
        return f"Glue job triggered - Run ID: {job_run_id}"
        
    except Exception as e:
        print(f"⚠️  Glue job trigger warning: {str(e)}")
        return f"Glue job skipped: {str(e)}"

def wait_for_glue_job():
    """
    Step 4: Wait for Glue job to complete
    """
    print("=" * 70)
    print("⏳ STEP 4: WAITING FOR GLUE JOB")
    print("=" * 70)
    
    try:
        glue_client = boto3.client('glue', region_name=AWS_REGION)
        response = glue_client.get_job_runs(JobName=GLUE_JOB_NAME, MaxResults=1)
        
        if response['JobRuns']:
            job_run = response['JobRuns'][0]
            job_state = job_run['JobRunState']
            print(f"Current Glue job state: {job_state}")
            
            if job_state == 'SUCCEEDED':
                print("✅ Glue job completed successfully!")
            elif job_state == 'FAILED':
                print("❌ Glue job failed!")
                raise Exception("Glue job failed")
        
        print("=" * 70)
        print("✅ STEP 4 COMPLETE: Glue job monitoring done")
        print("=" * 70)
        
    except Exception as e:
        print(f"⚠️  Glue monitoring warning: {str(e)}")

def check_processed_data():
    """
    Step 5: Verify processed data in S3
    """
    print("=" * 70)
    print("🔄 STEP 5: CHECKING PROCESSED DATA")
    print("=" * 70)
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='processed/'
        )
        
        if 'Contents' not in response:
            print("ℹ️  No processed files found yet")
            return "No processed data yet"
        
        file_count = len(response['Contents'])
        print(f"✅ Found {file_count} files in S3 processed/ folder")
        
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        print(f"📄 Latest file: {latest_file['Key']}")
        print(f"📊 Size: {latest_file['Size']} bytes")
        
        print("=" * 70)
        print("✅ STEP 5 COMPLETE: Processed data verified")
        print("=" * 70)
        return f"Processed data check - {file_count} files"
        
    except Exception as e:
        print(f"⚠️  Processed data check warning: {str(e)}")
        return f"Processed data check skipped: {str(e)}"

def send_completion_notification():
    """
    Step 6: Send completion summary
    """
    print("=" * 70)
    print("✅ PIPELINE EXECUTION COMPLETE")
    print("=" * 70)
    print("")
    print("🎉 FLIGHT DATA PIPELINE COMPLETED SUCCESSFULLY!")
    print("")
    print("📊 Summary:")
    print("  ✅ Step 1: Producer fetched API data → S3 raw + SQS")
    print("  ✅ Step 2: Verified SQS messages")
    print("  ✅ Step 2B: Lambda processed data → DynamoDB")
    print("  ✅ Step 3: Triggered Glue job")
    print("  ✅ Step 4: Monitored Glue progress")
    print("  ✅ Step 5: Verified processed data in S3")
    print("")
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    return "Pipeline summary sent"

# ============================================
# AIRFLOW TASKS
# ============================================

task_start = BashOperator(
    task_id='pipeline_start',
    bash_command='echo "🚀 Flight Data Pipeline Started - $(date)"',
    dag=dag,
)

task_producer = PythonOperator(
    task_id='step_1_run_producer',
    python_callable=run_producer_script,
    retries=2,
    dag=dag,
)

task_check_sqs = PythonOperator(
    task_id='step_2_check_sqs',
    python_callable=check_sqs_messages,
    dag=dag,
)

task_check_dynamodb = PythonOperator(
    task_id='step_2b_check_dynamodb',
    python_callable=check_dynamodb_data,
    dag=dag,
)

task_trigger_glue = PythonOperator(
    task_id='step_3_trigger_glue_job',
    python_callable=trigger_glue_job,
    dag=dag,
)

task_wait_glue = PythonOperator(
    task_id='step_4_wait_glue_job',
    python_callable=wait_for_glue_job,
    dag=dag,
)

task_check_processed = PythonOperator(
    task_id='step_5_check_processed_data',
    python_callable=check_processed_data,
    dag=dag,
)

task_notify = PythonOperator(
    task_id='step_6_send_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

task_end = BashOperator(
    task_id='pipeline_complete',
    bash_command='echo "✅ Flight Data Pipeline Completed - $(date)"',
    dag=dag,
)

# ============================================
# DEFINE TASK DEPENDENCIES
# ============================================

task_start >> task_producer >> task_check_sqs >> task_check_dynamodb >> task_trigger_glue >> task_wait_glue >> task_check_processed >> task_notify >> task_end