import boto3
import requests
import json
import time
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================

# Your API credentials
API_KEY = 'f55544b68e9a2701c92c8515adaf6b7e'  # From Aviation Stack
FLIGHT_API_URL = 'http://api.aviationstack.com/v1/flights'

# AWS Configuration (FROM TERRAFORM)
# ⚠️ UPDATE THESE VALUES FROM: terraform output
QUEUE_NAME = 'flight_dev'  # ✅ CHANGED: flight-queue → flight_dev
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/293732298754/flight_dev'  # ⚠️ REPLACE WITH YOUR VALUE
S3_BUCKET = 'flights-data-lake-amruta'  # ✅ Same as before
REGION = 'us-east-1'

# ============================================
# INITIALIZE AWS CLIENTS
# ============================================

sqs_client = boto3.client('sqs', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)

# ============================================
# FUNCTION: Get Queue URL (Auto-detect from Terraform)
# ============================================

def get_queue_url():
    """Automatically get the SQS queue URL from queue name"""
    try:
        response = sqs_client.get_queue_url(QueueName=QUEUE_NAME)
        queue_url = response['QueueUrl']
        print(f"✅ Found queue URL: {queue_url}")
        return queue_url
    except Exception as e:
        print(f"❌ Error getting queue URL: {e}")
        print(f"❌ Make sure queue name '{QUEUE_NAME}' exists")
        return None

# ============================================
# FUNCTION: Fetch Flight Data from API
# ============================================

def fetch_flight_data():
    """Fetch flight data from Aviation Stack API"""
    try:
        params = {
            'access_key': API_KEY,
            'limit': 10  # Get top 10 flights
        }
        
        response = requests.get(FLIGHT_API_URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            flights = data.get('data', [])
            print(f"✅ Fetched {len(flights)} flights from API")
            return flights
        else:
            print(f"❌ API Error: {response.status_code}")
            return []
    
    except Exception as e:
        print(f"❌ Error fetching from API: {e}")
        return []

# ============================================
# FUNCTION: Send to SQS Queue
# ============================================

def send_to_sqs(queue_url, flight_data):
    """Send flight data to SQS queue"""
    try:
        # Convert to JSON
        message_body = json.dumps(flight_data)
        
        # Send to SQS
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )
        
        message_id = response['MessageId']
        print(f"✅ Sent to SQS: {message_id}")
        return True
    
    except Exception as e:
        print(f"❌ Error sending to SQS: {e}")
        return False

# ============================================
# FUNCTION: Save to S3 (Raw Data Lake)
# ============================================

def save_to_s3(flight_data):
    """Save flight data to S3 raw layer"""
    try:
        # Create timestamp for file name
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_key = f'raw/flights_{timestamp}.json'
        
        # Save to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=file_key,
            Body=json.dumps(flight_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"✅ Saved to S3: s3://{S3_BUCKET}/{file_key}")
        return True
    
    except Exception as e:
        print(f"❌ Error saving to S3: {e}")
        return False

# ============================================
# FUNCTION: Process and Display Data
# ============================================

def process_flights(flights):
    """Transform and display flight data"""
    processed = []
    
    for flight in flights:
        try:
            flight_info = {
                'flight_id': flight.get('flight', {}).get('iata', 'N/A'),
                'airline': flight.get('airline', {}).get('name', 'N/A'),
                'departure': flight.get('departure', {}).get('airport', 'N/A'),
                'arrival': flight.get('arrival', {}).get('airport', 'N/A'),
                'status': flight.get('flight_status', 'N/A'),
                'timestamp': datetime.now().isoformat()
            }
            processed.append(flight_info)
            
            # Print to console
            print(f"\n📊 Flight: {flight_info['flight_id']}")
            print(f"   Airline: {flight_info['airline']}")
            print(f"   Route: {flight_info['departure']} → {flight_info['arrival']}")
            print(f"   Status: {flight_info['status']}")
        
        except Exception as e:
            print(f"Error processing flight: {e}")
    
    return processed

# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """Main producer function"""
    
    print("=" * 50)
    print("🚀 FLIGHT DATA PRODUCER STARTED")
    print("=" * 50)
    print(f"⏰ Started at: {datetime.now()}")
    
    # Get SQS Queue URL
    queue_url = get_queue_url()
    if not queue_url:
        print("❌ Could not get queue URL. Exiting.")
        return
    
    print(f"✅ Queue URL: {queue_url}")
    
    # Fetch flights
    print("\n📡 Fetching flight data from API...")
    flights = fetch_flight_data()
    
    if not flights:
        print("❌ No flights fetched. Exiting.")
        return
    
    # Process flights
    print("\n🔄 Processing flight data...")
    processed_flights = process_flights(flights)
    
    # Send to SQS (each flight individually)
    print("\n📤 Sending to SQS Queue...")
    for flight in processed_flights:
        send_to_sqs(queue_url, flight)
        time.sleep(0.5)  # Small delay between messages
    
    # Save to S3 (all flights together)
    print("\n💾 Saving to S3...")
    save_to_s3(processed_flights)
    
    print("\n" + "=" * 50)
    print("✅ PRODUCER COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print(f"📊 Processed: {len(processed_flights)} flights")
    print(f"⏰ Completed at: {datetime.now()}")

# ============================================
# RUN THE SCRIPT
# ============================================

if __name__ == "__main__":
    main()