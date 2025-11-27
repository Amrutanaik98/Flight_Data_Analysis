import boto3
import pandas as pd
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError
import pyarrow.parquet as pq
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

class FlightAnalytics:
    def __init__(self, region='us-east-1', bucket='flights-data-lake-amruta'):
        """Initialize AWS clients"""
        try:
            self.s3 = boto3.client('s3', region_name=region)
            self.bucket = bucket
            logger.info("✅ AWS clients initialized successfully")
        except Exception as e:
            logger.error(f"❌ Error initializing AWS clients: {e}")
            raise

    def fetch_flight_data_from_parquet(self):
        """Fetch flight data from Parquet files in S3"""
        try:
            logger.info("📊 Fetching flight data from Parquet files...")
            
            # List all parquet files in processed/flights_main
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix='processed/flights_main/',
                MaxKeys=100
            )
            
            if 'Contents' not in response or len(response['Contents']) == 0:
                logger.warning("⚠️ No parquet files found in processed/flights_main/")
                return pd.DataFrame()
            
            # Get all parquet files
            parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
            
            if not parquet_files:
                logger.warning("⚠️ No parquet files found")
                return pd.DataFrame()
            
            logger.info(f"✅ Found {len(parquet_files)} parquet files")
            
            # Read all parquet files and combine
            dfs = []
            for parquet_file in parquet_files:
                try:
                    logger.info(f"📖 Reading: {parquet_file}")
                    obj = self.s3.get_object(Bucket=self.bucket, Key=parquet_file)
                    df = pd.read_parquet(BytesIO(obj['Body'].read()))
                    dfs.append(df)
                except Exception as e:
                    logger.warning(f"⚠️ Error reading {parquet_file}: {e}")
                    continue
            
            if not dfs:
                logger.warning("⚠️ No data could be read from parquet files")
                return pd.DataFrame()
            
            # Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"✅ Retrieved {len(combined_df)} flights from Parquet files")
            
            return combined_df
            
        except Exception as e:
            logger.error(f"❌ Error fetching flight data: {e}")
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
                'columns_available': list(df.columns)
            }
            
            logger.info(f"📋 Available columns: {analytics['columns_available']}")
            
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
                logger.info(f"✅ Routes: {list(analytics['by_route'].keys())[:5]}...")
            
            return analytics
            
        except Exception as e:
            logger.error(f"❌ Error analyzing data: {e}")
            return None

    def save_report(self, analytics, bucket=None):
        """Save analytics report to S3"""
        try:
            if not analytics:
                logger.warning("⚠️ No analytics to save")
                return False
            
            if bucket is None:
                bucket = self.bucket
            
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
            logger.info(f"📍 Location: s3://{bucket}/{key}")
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
        analytics_engine = FlightAnalytics(bucket='flights-data-lake-amruta')
        
        # Fetch data from Parquet files
        df = analytics_engine.fetch_flight_data_from_parquet()
        
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
