import json
import boto3
from datetime import datetime
import os

# Initialize AWS Clients
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Get environment variables
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'flights-realtime-dev')
REGION = os.environ.get('AWS_REGION', 'us-east-1')

# DynamoDB Table
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    """Main Lambda handler to process SQS messages"""
    
    print(f"🚀 Lambda triggered at: {datetime.now()}")
    print(f"📊 Processing event: {json.dumps(event)}")
    
    try:
        records = event.get('Records', [])
        successful = 0
        failed = 0
        
        for record in records:
            try:
                message_id = record['messageId']
                receipt_handle = record['receiptHandle']
                body = record['body']
                
                print(f"\n📨 Processing message: {message_id}")
                
                # Parse JSON body
                flight_data = json.loads(body)
                
                # Validate required fields
                if not flight_data.get('flight_id'):
                    print(f"❌ Missing flight_id in message")
                    failed += 1
                    continue
                
                # Prepare DynamoDB item
                dynamo_item = {
                    'flight_id': flight_data['flight_id'],
                    'timestamp': flight_data.get('timestamp', datetime.now().isoformat()),
                    'airline': flight_data.get('airline', 'N/A'),
                    'departure': flight_data.get('departure', 'N/A'),
                    'arrival': flight_data.get('arrival', 'N/A'),
                    'status': flight_data.get('status', 'N/A'),
                    'received_at': datetime.now().isoformat(),
                    'message_id': message_id
                }
                
                # Write to DynamoDB
                table.put_item(Item=dynamo_item)
                
                print(f"✅ Written to DynamoDB: {flight_data['flight_id']}")
                successful += 1
                
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON: {e}")
                failed += 1
            except Exception as e:
                print(f"❌ Error processing record: {e}")
                failed += 1
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processed SQS records',
                'successful': successful,
                'failed': failed,
                'timestamp': datetime.now().isoformat()
            })
        }
        
        print(f"\n✅ Lambda completed successfully!")
        print(f"📊 Successful: {successful}")
        print(f"❌ Failed: {failed}")
        
        return response
    
    except Exception as e:
        print(f"❌ Fatal error in Lambda: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }