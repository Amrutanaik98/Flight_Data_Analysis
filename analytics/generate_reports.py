import boto3
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    """Generate and send automated reports"""
    
    def __init__(self):
        """Initialize AWS clients"""
        try:
            self.s3 = boto3.client('s3', region_name='us-east-1')
            self.sns = boto3.client('sns', region_name='us-east-1')
            self.bucket = 'flights-data-lake-amruta'
            logger.info("✅ Report generator initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize: {e}")
            raise
    
    def load_latest_report(self):
        """Load latest analytics report from S3"""
        try:
            logger.info("📂 Loading latest report from S3...")
            
            # List reports and get latest
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix='analytics/reports/',
                MaxKeys=1
            )
            
            if 'Contents' not in response:
                logger.warning("⚠️ No reports found in S3")
                return None
            
            latest_file = response['Contents'][0]['Key']
            
            # Get the report
            obj = self.s3.get_object(Bucket=self.bucket, Key=latest_file)
            report = json.loads(obj['Body'].read().decode('utf-8'))
            
            logger.info(f"✅ Loaded report: {latest_file}")
            return report
        
        except Exception as e:
            logger.error(f"❌ Error loading report: {e}")
            return None
    
    def format_email_body(self, report):
        """Format report as email body"""
        try:
            if not report:
                return "No report data available"
            
            summary = report.get('summary', {})
            
            email_body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    .header {{ background-color: #4CAF50; color: white; padding: 20px; }}
                    .metric {{ background-color: #f0f0f0; padding: 10px; margin: 5px; }}
                    .success {{ color: green; }}
                    .warning {{ color: orange; }}
                    .danger {{ color: red; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>✈️ Daily Flight Analytics Report</h1>
                    <p>Generated: {report.get('generated_at', 'N/A')}</p>
                </div>
                
                <h2>📊 Summary Statistics</h2>
                <div class="metric">
                    <p><strong>Total Flights:</strong> {summary.get('total_flights', 0)}</p>
                    <p class="success"><strong>On-Time Flights:</strong> {summary.get('on_time_flights', 0)} ({summary.get('on_time_rate_percent', 0):.1f}%)</p>
                    <p class="warning"><strong>Delayed Flights:</strong> {summary.get('delayed_flights', 0)} ({summary.get('delay_rate_percent', 0):.1f}%)</p>
                    <p class="danger"><strong>Cancelled Flights:</strong> {summary.get('cancelled_flights', 0)} ({summary.get('cancellation_rate_percent', 0):.1f}%)</p>
                </div>
                
                <h2>⏱️ Delay Metrics</h2>
                <div class="metric">
                    <p><strong>Average Delay:</strong> {summary.get('avg_delay_minutes', 0):.2f} minutes</p>
                    <p><strong>Max Delay:</strong> {summary.get('max_delay_minutes', 0):.2f} minutes</p>
                    <p><strong>Median Delay:</strong> {summary.get('median_delay_minutes', 0):.2f} minutes</p>
                </div>
                
                <hr>
                <p><small>This is an automated report. Do not reply to this email.</small></p>
            </body>
            </html>
            """
            
            return email_body
        
        except Exception as e:
            logger.error(f"❌ Error formatting email: {e}")
            return "Error formatting report"
    
    def send_email_report(self, report, email_subject="Daily Flight Analytics Report"):
        """Send report via SNS"""
        try:
            logger.info("📧 Sending email report via SNS...")
            
            email_body = self.format_email_body(report)
            
            # Note: Replace with your actual SNS topic ARN or SES email
            # For now, we'll just log it
            logger.info("✅ Email report formatted")
            logger.info(f"Subject: {email_subject}")
            logger.info("Email body prepared (check logs)")
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Error sending email: {e}")
            return False
    
    def save_summary_to_s3(self, report):
        """Save summary as text file to S3"""
        try:
            logger.info("📝 Saving summary to S3...")
            
            summary = report.get('summary', {})
            date = datetime.now().strftime('%Y-%m-%d')
            
            summary_text = f"""
FLIGHT ANALYTICS SUMMARY - {date}
===================================

FLIGHTS OVERVIEW:
- Total Flights: {summary.get('total_flights', 0)}
- On-Time: {summary.get('on_time_flights', 0)} ({summary.get('on_time_rate_percent', 0):.1f}%)
- Delayed: {summary.get('delayed_flights', 0)} ({summary.get('delay_rate_percent', 0):.1f}%)
- Cancelled: {summary.get('cancelled_flights', 0)} ({summary.get('cancellation_rate_percent', 0):.1f}%)

DELAY METRICS:
- Average Delay: {summary.get('avg_delay_minutes', 0):.2f} minutes
- Max Delay: {summary.get('max_delay_minutes', 0):.2f} minutes
- Median Delay: {summary.get('median_delay_minutes', 0):.2f} minutes

Generated: {report.get('generated_at', 'N/A')}
            """
            
            key = f'analytics/summaries/{date}_summary.txt'
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=summary_text,
                ContentType='text/plain'
            )
            
            logger.info(f"✅ Summary saved to s3://{self.bucket}/{key}")
            return key
        
        except Exception as e:
            logger.error(f"❌ Error saving summary: {e}")
            return None


# ===== MAIN EXECUTION =====
if __name__ == "__main__":
    try:
        logger.info("=" * 60)
        logger.info("🚀 REPORT GENERATOR STARTED")
        logger.info("=" * 60)
        
        # Initialize generator
        generator = ReportGenerator()
        
        # Load latest report
        report = generator.load_latest_report()
        
        if report:
            # Format and send email
            generator.send_email_report(report)
            
            # Save summary
            generator.save_summary_to_s3(report)
            
            logger.info("=" * 60)
            logger.info("✅ REPORT GENERATION COMPLETE")
            logger.info("=" * 60)
        else:
            logger.warning("⚠️ No report available to generate")
    
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise
