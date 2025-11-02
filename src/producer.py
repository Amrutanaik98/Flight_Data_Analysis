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

# ============================================
# INITIALIZE AWS CLIENTS
# ============================================

sqs_client = boto3.client('sqs', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)

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
# FUNCTION: Save to S3 (DETAILED JSONL FORMAT)
# ============================================

def save_to_s3(flight_data):
    """Save flight data to S3 raw layer in detailed JSONL format"""
    try:
        # Create timestamp for file name
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_key = f'raw/flights_{timestamp}.json'
        
        # Create detailed JSONL format (one JSON object per line)
        jsonl_lines = []
        for flight in flight_data:
            # Each flight is a separate line with comprehensive details
            flight_json = json.dumps(flight, separators=(',', ':'), ensure_ascii=False)
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
# FUNCTION: Process and Display Data (DETAILED)
# ============================================

def process_flights(flights):
    """Transform and display detailed flight data"""
    processed = []
    
    for flight in flights:
        try:
            # Safe get function to handle None values
            def safe_get(obj, key, default='N/A'):
                if obj is None:
                    return default
                return obj.get(key, default)
            
            flight_obj = flight.get('flight') or {}
            airline_obj = flight.get('airline') or {}
            aircraft_obj = flight.get('aircraft') or {}
            departure_obj = flight.get('departure') or {}
            arrival_obj = flight.get('arrival') or {}
            
            flight_info = {
                # Basic Flight Information
                'flight_id': safe_get(flight_obj, 'iata'),
                'flight_icao': safe_get(flight_obj, 'icao'),
                'flight_number': safe_get(flight_obj, 'number'),
                
                # Airline Information
                'airline_name': safe_get(airline_obj, 'name'),
                'airline_iata': safe_get(airline_obj, 'iata'),
                'airline_icao': safe_get(airline_obj, 'icao'),
                
                # Aircraft Information
                'aircraft_type': safe_get(aircraft_obj, 'iata'),
                'aircraft_icao': safe_get(aircraft_obj, 'icao'),
                'aircraft_name': safe_get(aircraft_obj, 'iata'),
                
                # Departure Details
                'departure_airport': safe_get(departure_obj, 'airport'),
                'departure_iata': safe_get(departure_obj, 'iata'),
                'departure_icao': safe_get(departure_obj, 'icao'),
                'departure_timezone': safe_get(departure_obj, 'timezone'),
                'departure_scheduled': safe_get(departure_obj, 'scheduled'),
                'departure_estimated': safe_get(departure_obj, 'estimated'),
                'departure_actual': safe_get(departure_obj, 'actual'),
                'departure_gate': safe_get(departure_obj, 'gate'),
                'departure_terminal': safe_get(departure_obj, 'terminal'),
                
                # Arrival Details
                'arrival_airport': safe_get(arrival_obj, 'airport'),
                'arrival_iata': safe_get(arrival_obj, 'iata'),
                'arrival_icao': safe_get(arrival_obj, 'icao'),
                'arrival_timezone': safe_get(arrival_obj, 'timezone'),
                'arrival_scheduled': safe_get(arrival_obj, 'scheduled'),
                'arrival_estimated': safe_get(arrival_obj, 'estimated'),
                'arrival_actual': safe_get(arrival_obj, 'actual'),
                'arrival_gate': safe_get(arrival_obj, 'gate'),
                'arrival_terminal': safe_get(arrival_obj, 'terminal'),
                
                # Flight Status
                'flight_status': flight.get('flight_status', 'N/A'),
                'live': flight.get('live', False),
                
                # Additional Details
                'codeshared_flight': safe_get(flight_obj, 'codeshared'),
                'is_codeshare': 'codeshared' in flight_obj,
                
                # Metadata
                'data_fetched_at': datetime.now().isoformat(),
                'source': 'Aviation Stack API',
                'raw_data': flight  # Store complete raw API response for reference
            }
            processed.append(flight_info)
            
            # Print to console
            print(f"\n📊 Flight: {flight_info['flight_id']}")
            print(f"   Airline: {flight_info['airline_name']} ({flight_info['airline_iata']})")
            print(f"   Aircraft: {flight_info['aircraft_type']}")
            print(f"   Route: {flight_info['departure_iata']} → {flight_info['arrival_iata']}")
            print(f"   Departure: {flight_info['departure_scheduled']} (Terminal: {flight_info['departure_terminal']})")
            print(f"   Arrival: {flight_info['arrival_scheduled']} (Gate: {flight_info['arrival_gate']})")
            print(f"   Status: {flight_info['flight_status']}")
        
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
    
    # Process flights with detailed information
    print("\n🔄 Processing flight data (detailed)...")
    processed_flights = process_flights(flights)
    
    # Send to SQS (each flight individually)
    print("\n📤 Sending to SQS Queue...")
    for flight in processed_flights:
        send_to_sqs(queue_url, flight)
        time.sleep(0.5)  # Small delay between messages
    
    # Save to S3 (all flights in detailed JSONL format)
    print("\n💾 Saving to S3...")
    save_to_s3(processed_flights)
    
    print("\n" + "=" * 50)
    print("✅ PRODUCER COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print(f"📊 Processed: {len(processed_flights)} flights")
    print(f"📤 Sent to SQS: {len(processed_flights)} messages")
    print(f"💾 Saved to S3: s3://{S3_BUCKET}/raw/flights_*.jsonl")
    print(f"⏰ Completed at: {datetime.now()}")
    print("=" * 50)

# ============================================
# RUN THE SCRIPT
# ============================================

if __name__ == "__main__":
    main()