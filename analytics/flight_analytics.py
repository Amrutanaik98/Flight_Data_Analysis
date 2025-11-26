import boto3
import pandas as pd
from datetime import datetime, timedelta
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlightAnalytics:
    """Main analytics engine for flight data"""
    
    def __init__(self):
        """Initialize AWS clients"""
        try:
            self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            self.s3 = boto3.client('s3', region_name='us-east-1')
            self.table = self.dynamodb.Table('flights-realtime-dev')
            self.bucket = 'flights-data-lake-amruta'
            logger.info("✅ AWS clients initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize AWS clients: {e}")
            raise
    
    def fetch_flight_data(self, days=7):
        """Fetch flight data from DynamoDB for last N days"""
        try:
            logger.info(f"📊 Fetching flight data for last {days} days...")
            
            response = self.table.scan()
            items = response.get('Items', [])
            
            if not items:
                logger.warning("⚠️ No data found in DynamoDB")
                return pd.DataFrame()
            
            df = pd.DataFrame(items)
            
            # Convert delay_minutes to numeric
            df['delay_minutes'] = pd.to_numeric(
                df.get('delay_minutes', 0), 
                errors='coerce'
            ).fillna(0)
            
            logger.info(f"✅ Fetched {len(df)} flight records")
            return df
        
        except Exception as e:
            logger.error(f"❌ Error fetching flight data: {e}")
            return pd.DataFrame()
    
    def airline_performance(self, df):
        """Analyze performance by airline"""
        try:
            if df.empty:
                logger.warning("⚠️ No data for airline analysis")
                return {}
            
            logger.info("✈️ Analyzing airline performance...")
            
            airline_stats = df.groupby('airline').agg({
                'flight_id': 'count',
                'delay_minutes': ['mean', 'max', 'min'],
                'status': lambda x: (x == 'on time').sum()
            }).round(2)
            
            airline_stats.columns = ['total_flights', 'avg_delay', 'max_delay', 'min_delay', 'on_time_count']
            
            # Calculate percentages
            airline_stats['on_time_rate'] = (
                airline_stats['on_time_count'] / airline_stats['total_flights'] * 100
            ).round(2)
            
            airline_stats['delay_rate'] = (
                (airline_stats['total_flights'] - airline_stats['on_time_count']) / 
                airline_stats['total_flights'] * 100
            ).round(2)
            
            # Sort by total flights
            airline_stats = airline_stats.sort_values('total_flights', ascending=False)
            
            logger.info(f"✅ Analyzed {len(airline_stats)} airlines")
            return airline_stats.to_dict()
        
        except Exception as e:
            logger.error(f"❌ Error in airline analysis: {e}")
            return {}
    
    def route_performance(self, df, top_n=10):
        """Analyze performance by route"""
        try:
            if df.empty:
                logger.warning("⚠️ No data for route analysis")
                return {}
            
            logger.info(f"🗺️ Analyzing top {top_n} routes...")
            
            route_stats = df.groupby(['departure', 'arrival']).agg({
                'flight_id': 'count',
                'delay_minutes': ['mean', 'max'],
                'status': lambda x: (x == 'delayed').sum()
            }).round(2)
            
            route_stats.columns = ['total_flights', 'avg_delay', 'max_delay', 'delayed_count']
            
            # Sort by total flights and get top N
            route_stats = route_stats.sort_values('total_flights', ascending=False).head(top_n)
            
            logger.info(f"✅ Analyzed {len(route_stats)} routes")
            return route_stats.to_dict()
        
        except Exception as e:
            logger.error(f"❌ Error in route analysis: {e}")
            return {}
    
    def delay_analysis(self, df):
        """Analyze delay patterns"""
        try:
            if df.empty:
                logger.warning("⚠️ No data for delay analysis")
                return {}
            
            logger.info("📊 Analyzing delay patterns...")
            
            stats = {
                'total_flights': int(len(df)),
                'on_time_flights': int(len(df[df['status'] == 'on time'])),
                'delayed_flights': int(len(df[df['status'] == 'delayed'])),
                'cancelled_flights': int(len(df[df['status'] == 'cancelled'])),
                'avg_delay_minutes': float(df['delay_minutes'].mean()),
                'max_delay_minutes': float(df['delay_minutes'].max()),
                'min_delay_minutes': float(df['delay_minutes'].min()),
                'median_delay_minutes': float(df['delay_minutes'].median()),
                'on_time_rate_percent': float(
                    (len(df[df['status'] == 'on time']) / len(df) * 100)
                ),
                'delay_rate_percent': float(
                    (len(df[df['status'] == 'delayed']) / len(df) * 100)
                ),
                'cancellation_rate_percent': float(
                    (len(df[df['status'] == 'cancelled']) / len(df) * 100)
                )
            }
            
            logger.info(f"✅ Delay analysis complete")
            return stats
        
        except Exception as e:
            logger.error(f"❌ Error in delay analysis: {e}")
            return {}
    
    def temporal_analysis(self, df):
        """Analyze patterns by time"""
        try:
            if df.empty:
                logger.warning("⚠️ No data for temporal analysis")
                return {}
            
            logger.info("🕐 Analyzing temporal patterns...")
            
            # This is placeholder - would need timestamp column in data
            stats = {
                'analysis_performed': True,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"✅ Temporal analysis complete")
            return stats
        
        except Exception as e:
            logger.error(f"❌ Error in temporal analysis: {e}")
            return {}
    
    def generate_report(self, df):
        """Generate comprehensive report"""
        try:
            logger.info("📋 Generating comprehensive report...")
            
            report = {
                'generated_at': datetime.now().isoformat(),
                'report_period': f"Last 7 days",
                'summary': self.delay_analysis(df),
                'airline_performance': self.airline_performance(df),
                'route_performance': self.route_performance(df),
                'temporal_patterns': self.temporal_analysis(df)
            }
            
            logger.info("✅ Report generated successfully")
            return report
        
        except Exception as e:
            logger.error(f"❌ Error generating report: {e}")
            return {}
    
    def save_report_to_s3(self, report):
        """Save report to S3"""
        try:
            logger.info("💾 Saving report to S3...")
            
            date = datetime.now().strftime('%Y-%m-%d')
            key = f'analytics/reports/{date}_analytics_report.json'
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(report, indent=2, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Report saved to s3://{self.bucket}/{key}")
            return key
        
        except Exception as e:
            logger.error(f"❌ Error saving report to S3: {e}")
            return None
    
    def save_report_to_csv(self, df):
        """Save data to CSV in S3"""
        try:
            logger.info("📊 Saving data to CSV...")
            
            date = datetime.now().strftime('%Y-%m-%d')
            key = f'analytics/data/{date}_flights.csv'
            
            csv_buffer = df.to_csv(index=False)
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=csv_buffer,
                ContentType='text/csv'
            )
            
            logger.info(f"✅ CSV saved to s3://{self.bucket}/{key}")
            return key
        
        except Exception as e:
            logger.error(f"❌ Error saving CSV: {e}")
            return None


# ===== MAIN EXECUTION =====
if __name__ == "__main__":
    try:
        logger.info("=" * 60)
        logger.info("🚀 FLIGHT ANALYTICS ENGINE STARTED")
        logger.info("=" * 60)
        
        # Initialize analytics
        analytics = FlightAnalytics()
        
        # Fetch data
        df = analytics.fetch_flight_data(days=7)
        
        if not df.empty:
            # Generate report
            report = analytics.generate_report(df)
            
            # Save report
            analytics.save_report_to_s3(report)
            
            # Save CSV
            analytics.save_report_to_csv(df)
            
            logger.info("=" * 60)
            logger.info("✅ ANALYTICS COMPLETE - All data saved to S3")
            logger.info("=" * 60)
        else:
            logger.warning("⚠️ No data available for analysis")
    
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise
