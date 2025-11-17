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
QUEUE_NAME = 'flight_dev'
S3_BUCKET = 'flights-data-lake-amruta'
REGION = 'us-east-1'
DYNAMODB_TABLE = 'flight_data'  # Your DynamoDB table

# ============================================
# INITIALIZE AWS CLIENTS
# ============================================

sqs_client = boto3.client('sqs', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)

# ============================================
# FUNCTION: Get Queue URL
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
        return None

# ============================================
# FUNCTION: Check if Flight Already Exists
# ============================================

def flight_already_exists(flight_id):
    """Check if flight already exists in DynamoDB to avoid duplicates"""
    try:
        # Try to get the table
        table = dynamodb.Table(DYNAMODB_TABLE)
        response = table.get_item(
            Key={'flight_id': flight_id}
        )
        # If 'Item' exists, flight is already in DB
        return 'Item' in response
    except Exception as e:
        # If DynamoDB table doesn't exist, just process all flights
        # (They will be deduplicated once data flows through your pipeline)
        print(f"ℹ️  DynamoDB check skipped: {str(e)[:50]}")
        return False

# ============================================
# FUNCTION: Fetch Flight Data from API
# ============================================

def fetch_flight_data():
    """Fetch flight data from Aviation Stack API with error handling"""
    try:
        params = {
            'access_key': API_KEY,
            'limit': 50  # Get more flights to have better variety
        }
        
        print(f"📡 Making API request to: {FLIGHT_API_URL}")
        print(f"   Parameters: {params}")
        
        response = requests.get(FLIGHT_API_URL, params=params, timeout=10)
        
        print(f"   HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Check for API errors in response
            if data.get('error'):
                error_info = data.get('error', {})
                print(f"❌ API Error: {error_info.get('info', 'Unknown error')}")
                return []
            
            flights = data.get('data', [])
            print(f"✅ API returned {len(flights)} flights")
            
            # Filter out empty/invalid flights
            valid_flights = []
            for flight in flights:
                # Get flight details (more lenient validation)
                flight_num = flight.get('flight', {}).get('iata')
                airline_name = flight.get('airline', {}).get('name')
                departure_airport = flight.get('departure', {}).get('airport')
                arrival_airport = flight.get('arrival', {}).get('airport')
                
                # Accept flight if it has at least: departure + arrival airports
                # (don't require flight number or airline name to be perfect)
                if departure_airport and arrival_airport and departure_airport != 'empty' and arrival_airport != 'empty':
                    valid_flights.append(flight)
                else:
                    # Only show details for critically invalid flights
                    if not departure_airport or not arrival_airport:
                        print(f"⚠️  Skipped: Missing departure or arrival airport")
            
            print(f"✅ Valid flights: {len(valid_flights)} (after filtering empty)")
            return valid_flights
        
        else:
            print(f"❌ API HTTP Error: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return []
    
    except requests.exceptions.Timeout:
        print(f"❌ API Request Timeout - Server took too long to respond")
        return []
    
    except requests.exceptions.ConnectionError:
        print(f"❌ Connection Error - Cannot reach API server")
        return []
    
    except Exception as e:
        print(f"❌ Error fetching from API: {type(e).__name__}: {e}")
        return []

# ============================================
# FUNCTION: Send to SQS Queue
# ============================================

def send_to_sqs(queue_url, flight_data):
    """Send flight data to SQS queue"""
    try:
        # Convert to JSON string
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
# FUNCTION: Save to S3 (JSONL Format)
# ============================================

def save_to_s3(flight_data):
    """Save flight data to S3 raw layer in proper JSONL format"""
    try:
        if not flight_data:
            print("⚠️  No new flights to save to S3")
            return True
        
        # Create timestamp for file name
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_key = f'raw/flights_{timestamp}.json'
        
        # Create JSONL format (one JSON object per line)
        jsonl_lines = []
        for flight in flight_data:
            # Ensure each flight is a valid JSON object on its own line
            flight_json = json.dumps(flight, separators=(',', ':'))  # Compact format
            jsonl_lines.append(flight_json)
        
        # Join with newlines (JSONL format)
        jsonl_content = '\n'.join(jsonl_lines)
        
        # Save to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=file_key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"✅ Saved to S3: s3://{S3_BUCKET}/{file_key}")
        print(f"   Format: JSONL (one JSON object per line)")
        print(f"   Records: {len(flight_data)}")
        return True
    
    except Exception as e:
        print(f"❌ Error saving to S3: {e}")
        return False

# ============================================
# FUNCTION: Process and Display Data
# ============================================

def process_flights(flights):
    """Transform and display flight data, filtering duplicates"""
    processed = []
    skipped = 0
    
    for flight in flights:
        try:
            # Get flight ID - use IATA code, fallback to ICAO if not available
            flight_id = (flight.get('flight', {}).get('iata') or 
                        flight.get('flight', {}).get('icao') or 
                        'UNKNOWN')
            
            # Get airline name - handle 'empty' value
            airline_name = flight.get('airline', {}).get('name', 'N/A')
            if airline_name == 'empty' or not airline_name:
                airline_name = 'Unknown Airline'
            
            # 🔑 CHECK IF FLIGHT ALREADY EXISTS
            if flight_already_exists(flight_id):
                print(f"⏭️  SKIPPED: Flight {flight_id} already processed")
                skipped += 1
                continue
            
            flight_info = {
                'flight_id': flight_id,
                'airline': airline_name,
                'departure': flight.get('departure', {}).get('airport', 'N/A'),
                'arrival': flight.get('arrival', {}).get('airport', 'N/A'),
                'status': flight.get('flight_status', 'N/A'),
                'timestamp': datetime.now().isoformat()
            }
            processed.append(flight_info)
            
            # Print to console
            print(f"\n📊 NEW Flight: {flight_info['flight_id']}")
            print(f"   Airline: {flight_info['airline']}")
            print(f"   Route: {flight_info['departure']} → {flight_info['arrival']}")
            print(f"   Status: {flight_info['status']}")
        
        except Exception as e:
            print(f"Error processing flight: {e}")
    
    print(f"\n📈 Summary: {len(processed)} NEW flights, {skipped} SKIPPED (duplicates)")
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
    
    # Process flights (with deduplication)
    print("\n🔄 Processing flight data (checking for duplicates)...")
    processed_flights = process_flights(flights)
    
    # If all flights are duplicates, exit early
    if not processed_flights:
        print("\n⚠️  All flights are duplicates. Nothing new to process.")
        print("=" * 50)
        return
    
    # Send to SQS (each flight individually)
    print("\n📤 Sending NEW flights to SQS Queue...")
    for flight in processed_flights:
        send_to_sqs(queue_url, flight)
        time.sleep(0.5)  # Small delay between messages
    
    # Save to S3 (only new flights in JSONL format)
    print("\n💾 Saving NEW flights to S3...")
    save_to_s3(processed_flights)
    
    print("\n" + "=" * 50)
    print("✅ PRODUCER COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print(f"📊 Processed: {len(processed_flights)} NEW flights")
    print(f"📤 Sent to SQS: {len(processed_flights)} messages")
    print(f"💾 Saved to S3: s3://{S3_BUCKET}/raw/flights_*.json")
    print(f"⏰ Completed at: {datetime.now()}")
    print("=" * 50)

# ============================================
# RUN THE SCRIPT
# ============================================

if __name__ == "__main__":
    main()