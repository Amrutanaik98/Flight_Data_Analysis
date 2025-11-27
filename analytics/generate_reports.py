import boto3
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self, region='us-east-1', bucket='flights-data-lake-amruta'):
        """Initialize AWS clients"""
        try:
            self.s3 = boto3.client('s3', region_name=region)
            self.bucket = bucket
            logger.info("✅ AWS S3 client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Error initializing S3 client: {e}")
            raise

    def get_latest_analytics(self):
        """Get the latest analytics report from S3"""
        try:
            logger.info("📂 Fetching latest analytics report from S3...")
            
            # List objects in analytics/reports folder
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix='analytics/reports/',
                MaxKeys=100
            )
            
            if 'Contents' not in response or len(response['Contents']) == 0:
                logger.warning("⚠️ No analytics reports found in S3")
                return None
            
            # Get the latest file (most recently modified)
            latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]
            latest_key = latest_file['Key']
            
            logger.info(f"✅ Found latest report: {latest_key}")
            
            # Download and parse the JSON
            obj = self.s3.get_object(Bucket=self.bucket, Key=latest_key)
            analytics = json.loads(obj['Body'].read().decode('utf-8'))
            
            return analytics
            
        except ClientError as e:
            logger.error(f"❌ Error fetching analytics from S3: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error fetching analytics: {e}")
            return None

    def format_report(self, analytics):
        """Format analytics into a readable text report"""
        try:
            if not analytics:
                logger.warning("⚠️ No analytics to format")
                return None
            
            logger.info("📝 Formatting report...")
            
            # Build the report
            report = []
            report.append("=" * 70)
            report.append("FLIGHT DATA ANALYTICS REPORT")
            report.append("=" * 70)
            report.append("")
            
            # Metadata
            timestamp = analytics.get('analysis_timestamp', 'N/A')
            total_flights = analytics.get('total_flights', 0)
            report.append(f"Report Generated: {timestamp}")
            report.append(f"Total Flights Analyzed: {total_flights}")
            report.append("")
            
            # Airlines breakdown
            if analytics.get('by_airline'):
                report.append("-" * 70)
                report.append("FLIGHTS BY AIRLINE")
                report.append("-" * 70)
                for airline, count in sorted(analytics['by_airline'].items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / total_flights * 100) if total_flights > 0 else 0
                    report.append(f"  {airline}: {count} flights ({percentage:.1f}%)")
                report.append("")
            
            # Status breakdown
            if analytics.get('by_status'):
                report.append("-" * 70)
                report.append("FLIGHTS BY STATUS")
                report.append("-" * 70)
                for status, count in sorted(analytics['by_status'].items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / total_flights * 100) if total_flights > 0 else 0
                    report.append(f"  {status}: {count} flights ({percentage:.1f}%)")
                report.append("")
            
            # Routes breakdown
            if analytics.get('by_route'):
                report.append("-" * 70)
                report.append("TOP ROUTES")
                report.append("-" * 70)
                routes = sorted(analytics['by_route'].items(), key=lambda x: x[1], reverse=True)
                for i, (route, count) in enumerate(routes[:10], 1):  # Top 10 routes
                    report.append(f"  {i}. {route}: {count} flights")
                
                if len(routes) > 10:
                    report.append(f"  ... and {len(routes) - 10} more routes")
                report.append("")
            
            # Footer
            report.append("=" * 70)
            report.append("END OF REPORT")
            report.append("=" * 70)
            
            # Join all lines
            formatted_report = "\n".join(report)
            logger.info("✅ Report formatted successfully")
            
            return formatted_report
            
        except Exception as e:
            logger.error(f"❌ Error formatting report: {e}")
            return None

    def save_report(self, report_text):
        """Save formatted report to S3"""
        try:
            if not report_text:
                logger.warning("⚠️ No report to save")
                return False
            
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            key = f'analytics/summaries/flight_report_{timestamp}.txt'
            
            logger.info(f"💾 Saving report to S3: s3://{self.bucket}/{key}")
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=report_text.encode('utf-8'),
                ContentType='text/plain'
            )
            
            logger.info(f"✅ Report saved successfully")
            logger.info(f"📍 Location: s3://{self.bucket}/{key}")
            
            return True
            
        except ClientError as e:
            logger.error(f"❌ Error saving to S3: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error saving report: {e}")
            return False

    def print_report(self, report_text):
        """Print report to console"""
        if report_text:
            print("\n")
            print(report_text)
            print("\n")

def main():
    """Main execution"""
    logger.info("=" * 70)
    logger.info("🚀 REPORT GENERATOR STARTED")
    logger.info("=" * 70)
    
    try:
        # Initialize report generator
        generator = ReportGenerator(bucket='flights-data-lake-amruta')
        
        # Get latest analytics
        analytics = generator.get_latest_analytics()
        
        if not analytics:
            logger.warning("⚠️ No analytics available to generate report")
            return
        
        # Format report
        report = generator.format_report(analytics)
        
        if not report:
            logger.error("❌ Failed to format report")
            return
        
        # Print to console
        generator.print_report(report)
        
        # Save to S3
        generator.save_report(report)
        
        logger.info("✅ Report generation completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    
    logger.info("=" * 70)
    logger.info("🏁 REPORT GENERATOR FINISHED")
    logger.info("=" * 70)

if __name__ == '__main__':
    main()
