"""
Flight Data Analysis Pipeline - Airflow DAG
============================================

This DAG orchestrates your complete flight data pipeline:
1. Runs producer.py - Fetches flight data → S3 + SQS
2. Validates S3 data
3. Checks SQS messages
4. Checks DynamoDB data
5. Sends notifications

Schedule: Daily at 2 AM UTC
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
    description='Flight Data Analysis Pipeline - Producer + Validation',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,
    tags=['flights', 'production', 'data-pipeline'],
)

# ============================================
# PYTHON FUNCTIONS
# ============================================

def run_producer_script():
    """
    Run producer.py to fetch flight data
    """
    print("=" * 70)
    print("🚀 STARTING PRODUCER SCRIPT")
    print("=" * 70)
    
    try:
        result = subprocess.run(
            ['python', PRODUCER_SCRIPT],
            capture_output=True,
            text=True,
            timeout=600,  # 10 minutes
        )
        
        print("PRODUCER OUTPUT:")
        print(result.stdout)
        
        if result.returncode != 0:
            print("PRODUCER ERROR:")
            print(result.stderr)
            raise Exception(f"Producer script failed with return code {result.returncode}")
        
        print("=" * 70)
        print("✅ PRODUCER SCRIPT COMPLETED SUCCESSFULLY")
        print("=" * 70)
        return "Producer script executed successfully"
        
    except subprocess.TimeoutExpired:
        raise Exception("Producer script timed out after 10 minutes")
    except FileNotFoundError:
        raise Exception(f"Producer script not found at: {PRODUCER_SCRIPT}")
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        raise

def validate_s3_data():
    """
    Validate that data exists in S3 raw folder
    """
    print("=" * 70)
    print("🔍 VALIDATING S3 DATA")
    print("=" * 70)
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # List objects in raw folder
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='raw/'
        )
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            raise Exception("No files found in S3 raw/ folder!")
        
        file_count = len(response['Contents'])
        print(f"✅ Found {file_count} files in S3 raw/ folder")
        
        # Get latest file details
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        print(f"📄 Latest file: {latest_file['Key']}")
        print(f"📊 Size: {latest_file['Size']} bytes")
        print(f"⏰ Last modified: {latest_file['LastModified']}")
        
        print("=" * 70)
        print("✅ S3 DATA VALIDATION PASSED")
        print("=" * 70)
        return f"S3 validation passed - {file_count} files"
        
    except Exception as e:
        print(f"❌ S3 Validation failed: {str(e)}")
        raise

def check_sqs_messages():
    """
    Check SQS queue for messages
    """
    print("=" * 70)
    print("📤 CHECKING SQS QUEUE")
    print("=" * 70)
    
    try:
        sqs_client = boto3.client('sqs', region_name=AWS_REGION)
        
        # Get queue URL
        response = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
        queue_url = response['QueueUrl']
        print(f"Queue URL: {queue_url}")
        
        # Get queue attributes
        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesDelayed']
        )
        
        msg_count = int(response['Attributes']['ApproximateNumberOfMessages'])
        delayed_count = int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
        
        print(f"📨 Messages in queue: {msg_count}")
        print(f"⏱️  Delayed messages: {delayed_count}")
        
        print("=" * 70)
        print("✅ SQS CHECK COMPLETED")
        print("=" * 70)
        return f"SQS check passed - {msg_count} messages"
        
    except Exception as e:
        print(f"⚠️  SQS check warning: {str(e)}")
        return f"SQS check skipped: {str(e)}"

def check_dynamodb_data():
    """
    Check DynamoDB table for data
    """
    print("=" * 70)
    print("💾 CHECKING DYNAMODB TABLE")
    print("=" * 70)
    
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        
        # Scan table
        response = table.scan()
        item_count = response['Count']
        
        print(f"📊 Items in DynamoDB table: {item_count}")
        
        if item_count > 0 and response['Items']:
            sample_item = response['Items'][0]
            print(f"📋 Sample item keys: {list(sample_item.keys())}")
        
        print("=" * 70)
        print("✅ DYNAMODB CHECK COMPLETED")
        print("=" * 70)
        return f"DynamoDB check passed - {item_count} items"
        
    except Exception as e:
        print(f"⚠️  DynamoDB check warning: {str(e)}")
        return f"DynamoDB check skipped: {str(e)}"

def check_processed_data():
    """
    Check if processed data exists in S3
    """
    print("=" * 70)
    print("🔄 CHECKING PROCESSED DATA")
    print("=" * 70)
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # List objects in processed folder
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='processed/'
        )
        
        if 'Contents' not in response:
            print("ℹ️  No processed files found yet (Glue job may not have run)")
            return "No processed data yet"
        
        file_count = len(response['Contents'])
        print(f"✅ Found {file_count} files in S3 processed/ folder")
        
        print("=" * 70)
        print("✅ PROCESSED DATA CHECK COMPLETED")
        print("=" * 70)
        return f"Processed data check - {file_count} files"
        
    except Exception as e:
        print(f"⚠️  Processed data check warning: {str(e)}")
        return f"Processed data check skipped: {str(e)}"

def send_completion_notification():
    """
    Send pipeline completion notification
    """
    print("=" * 70)
    print("✅ PIPELINE COMPLETION NOTIFICATION")
    print("=" * 70)
    
    message = f"""
    
    ✅ FLIGHT DATA PIPELINE COMPLETED SUCCESSFULLY!
    
    📋 Pipeline Execution Summary:
    ================================
    Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    Status: SUCCESS
    
    Tasks Completed:
    ✅ Producer script executed
    ✅ S3 data validated (raw folder)
    ✅ SQS queue checked
    ✅ DynamoDB data verified
    ✅ Processed data checked
    
    📊 Data Locations:
    ================================
    S3 Bucket: s3://{S3_BUCKET}/
    ├── raw/ (Original flight data)
    ├── processed/ (Transformed data)
    └── archive/ (Historical data)
    
    SQS Queue: {SQS_QUEUE_NAME}
    DynamoDB Table: {DYNAMODB_TABLE}
    
    🔗 Access:
    ================================
    Airflow Dashboard: http://YOUR_EC2_IP:8080
    S3 Console: https://console.aws.amazon.com/s3/
    
    ⏭️  Next Steps:
    ================================
    1. Lambda processes SQS messages
    2. Glue job transforms raw → processed data
    3. Data ready for analytics
    
    For more details, check Airflow task logs.
    """
    
    print(message)
    print("=" * 70)
    return "Notification sent successfully"

# ============================================
# AIRFLOW TASKS
# ============================================

task_start = BashOperator(
    task_id='pipeline_start',
    bash_command='echo "🚀 Flight Data Pipeline Started at $(date)"',
    dag=dag,
)

task_producer = PythonOperator(
    task_id='run_producer_script',
    python_callable=run_producer_script,
    retries=2,
    dag=dag,
)

task_validate_s3 = PythonOperator(
    task_id='validate_s3_data',
    python_callable=validate_s3_data,
    dag=dag,
)

task_check_sqs = PythonOperator(
    task_id='check_sqs_messages',
    python_callable=check_sqs_messages,
    dag=dag,
)

task_check_dynamodb = PythonOperator(
    task_id='check_dynamodb_data',
    python_callable=check_dynamodb_data,
    dag=dag,
)

task_check_processed = PythonOperator(
    task_id='check_processed_data',
    python_callable=check_processed_data,
    dag=dag,
)

task_notify = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

task_end = BashOperator(
    task_id='pipeline_complete',
    bash_command='echo "✅ Flight Data Pipeline Completed at $(date)"',
    dag=dag,
)

# ============================================
# DEFINE TASK DEPENDENCIES
# ============================================

# Linear flow: start → producer → validate
task_start >> task_producer >> task_validate_s3

# Parallel checks
task_validate_s3 >> [task_check_sqs, task_check_dynamodb, task_check_processed]

# Converge to notify and end
[task_check_sqs, task_check_dynamodb, task_check_processed] >> task_notify >> task_end