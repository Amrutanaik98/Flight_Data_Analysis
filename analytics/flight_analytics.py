import boto3
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

class FlightAnalytics:
    def __init__(self, region='us-east-1'):
        """Initialize AWS clients"""
        try:
            self.dynamodb = boto3.resource('dynamodb', region_name=region)
            self.s3 = boto3.client('s3', region_name=region)
            logger.info("✅ AWS clients initialized successfully")
        except Exception as e:
            logger.error(f"❌ Error initializing AWS clients: {e}")
            raise

    def fetch_flight_data(self, days=7):
        """Fetch flight data from DynamoDB"""
        try:
            logger.info(f"📊 Fetching flight data for last {days} days...")
            
            # Try to get table
            table = self.dynamodb.Table('flight_data')
            
            # Scan the table
            response = table.scan()
            
            if not response.get('Items'):
                logger.warning("⚠️ No data available in DynamoDB")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(response['Items'])
            logger.info(f"✅ Retrieved {len(df)} flights from DynamoDB")
            
            return df
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(f"❌ DynamoDB table not found. Available tables:")
                try:
                    tables = self.dynamodb.meta.client.list_tables()
                    logger.info(f"Available tables: {tables['TableNames']}")
                except:
                    pass
            else:
                logger.error(f"❌ Error fetching flight data: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ Unexpected error fetching data: {e}")
            return pd.DataFrame()

    def analyze_data(self, df):
        """Perform analytics on flight data"""
        try:
            if df.empty:
                logger.warning("⚠️ No data to analyze")
                return None
            
            logger.info("🔍 Analyzing flight data...")
            
            analytics = {
                'total_flights': int(len(df)),
                'analysis_timestamp': datetime.now().isoformat(),
                'by_airline': {},
                'by_status': {},
                'by_route': {},
            }
            
            # Analyze by airline
            if 'airline' in df.columns:
                airline_counts = df['airline'].value_counts().to_dict()
                analytics['by_airline'] = {str(k): int(v) for k, v in airline_counts.items()}
                logger.info(f"✅ Airlines: {analytics['by_airline']}")
            
            # Analyze by status
            if 'status' in df.columns:
                status_counts = df['status'].value_counts().to_dict()
                analytics['by_status'] = {str(k): int(v) for k, v in status_counts.items()}
                logger.info(f"✅ Status: {analytics['by_status']}")
            
            # Analyze by route
            if 'departure' in df.columns and 'arrival' in df.columns:
                df['route'] = df['departure'].astype(str) + ' → ' + df['arrival'].astype(str)
                route_counts = df['route'].value_counts().to_dict()
                analytics['by_route'] = {str(k): int(v) for k, v in route_counts.items()}
                logger.info(f"✅ Routes: {analytics['by_route']}")
            
            return analytics
            
        except Exception as e:
            logger.error(f"❌ Error analyzing data: {e}")
            return None

    def save_report(self, analytics, bucket='flights-data-lake-amruta-2025'):
        """Save analytics report to S3"""
        try:
            if not analytics:
                logger.warning("⚠️ No analytics to save")
                return False
            
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            key = f'analytics/reports/flight_analytics_{timestamp}.json'
            
            logger.info(f"💾 Saving report to S3: s3://{bucket}/{key}")
            
            self.s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(analytics, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Report saved successfully")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Error saving to S3: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error saving report: {e}")
            return False

def main():
    """Main execution"""
    logger.info("=" * 60)
    logger.info("🚀 FLIGHT ANALYTICS ENGINE STARTED")
    logger.info("=" * 60)
    
    try:
        # Initialize analytics
        analytics_engine = FlightAnalytics()
        
        # Fetch data
        df = analytics_engine.fetch_flight_data(days=7)
        
        # Analyze
        analytics = analytics_engine.analyze_data(df)
        
        # Save report
        if analytics:
            analytics_engine.save_report(analytics)
            logger.info("✅ Analytics pipeline completed successfully")
        else:
            logger.warning("⚠️ Analytics pipeline completed with no data")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    
    logger.info("=" * 60)
    logger.info("🏁 FLIGHT ANALYTICS ENGINE FINISHED")
    logger.info("=" * 60)

if __name__ == '__main__':
    main()
