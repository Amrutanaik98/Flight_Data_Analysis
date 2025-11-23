import requests
import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

class AlertNotifier:
    """Send alerts via Slack, Email (SNS), and CloudWatch"""
    
    def __init__(self, slack_webhook_url=None, sns_topic_arn=None):
        self.slack_webhook_url = slack_webhook_url
        self.sns_topic_arn = sns_topic_arn
        self.sns = boto3.client('sns', region_name='us-east-1')
        self.cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
    
    # ============================================
    # SLACK ALERTS
    # ============================================
    
    def send_slack_alert(self, message, severity='info', title=None):
        """Send alert to Slack"""
        
        if not self.slack_webhook_url:
            print("⚠️  Slack webhook not configured")
            return False
        
        colors = {
            'critical': '#FF0000',  # Red
            'warning': '#FFA500',   # Orange
            'info': '#0099CC',      # Blue
            'success': '#36A64F'    # Green
        }
        
        payload = {
            "attachments": [
                {
                    "color": colors.get(severity, '#0099CC'),
                    "title": title or f"🚨 Flight Data Alert [{severity.upper()}]",
                    "text": message,
                    "footer": "Flight Data Pipeline",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        try:
            response = requests.post(
                self.slack_webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                print(f"✅ Slack alert sent: {title or severity}")
                return True
            else:
                print(f"❌ Slack failed ({response.status_code}): {response.text[:100]}")
                return False
        
        except requests.exceptions.Timeout:
            print("❌ Slack request timeout")
            return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Slack connection error: {str(e)[:100]}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error sending Slack: {str(e)[:100]}")
            return False
    
    # ============================================
    # EMAIL ALERTS (via SNS)
    # ============================================
    
    def send_email_alert(self, subject, message, severity='info'):
        """Send alert via SNS (Email)"""
        
        if not self.sns_topic_arn:
            print("⚠️  SNS topic not configured")
            return False
        
        try:
            email_subject = f"[{severity.upper()}] {subject}"
            
            response = self.sns.publish(
                TopicArn=self.sns_topic_arn,
                Subject=email_subject,
                Message=message
            )
            
            print(f"✅ Email sent via SNS: {subject}")
            return True
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NotFound':
                print(f"❌ SNS topic not found: {self.sns_topic_arn}")
            else:
                print(f"❌ SNS error ({error_code}): {e.response['Error']['Message']}")
            return False
        except Exception as e:
            print(f"❌ Error sending email: {str(e)[:100]}")
            return False
    
    # ============================================
    # QUALITY CHECK ALERTS
    # ============================================
    
    def alert_quality_issues(self, issues, severity='warning'):
        """Alert for data quality issues"""
        
        critical_issues = [i for i in issues if i.get('severity') == 'CRITICAL']
        warning_issues = [i for i in issues if i.get('severity') == 'WARNING']
        
        message = f"""
🚨 DATA QUALITY ALERT

Critical Issues: {len(critical_issues)}
Warning Issues: {len(warning_issues)}
Time: {datetime.now().isoformat()}

CRITICAL ISSUES:
"""
        for issue in critical_issues:
            message += f"  ❌ {issue.get('message', 'Unknown issue')}\n"
        
        message += "\nWARNING ISSUES:\n"
        for issue in warning_issues:
            message += f"  ⚠️  {issue.get('message', 'Unknown issue')}\n"
        
        # Send to Slack
        if critical_issues:
            self.send_slack_alert(message, severity='critical', title='🚨 Critical Data Quality Issues')
        else:
            self.send_slack_alert(message, severity='warning', title='⚠️ Data Quality Warnings')
        
        # Send email if critical
        if critical_issues:
            self.send_email_alert(
                subject='Critical Data Quality Issues',
                message=message,
                severity='critical'
            )
    
    # ============================================
    # PIPELINE SUCCESS/FAILURE ALERTS
    # ============================================
    
    def alert_pipeline_success(self, task_name, duration_seconds):
        """Alert for successful pipeline execution"""
        
        message = f"""
✅ PIPELINE SUCCESS

Task: {task_name}
Duration: {duration_seconds:.2f} seconds
Time: {datetime.now().isoformat()}

All checks passed. Data is ready for analysis.
"""
        
        self.send_slack_alert(message, severity='success', title='✅ Pipeline Completed Successfully')
    
    def alert_pipeline_failure(self, task_name, error_message, error_type='Error'):
        """Alert for pipeline failures"""
        
        message = f"""
❌ PIPELINE FAILURE

Task: {task_name}
Error Type: {error_type}
Error Message: {error_message}
Time: {datetime.now().isoformat()}

Action Required: Check logs immediately!
"""
        
        self.send_slack_alert(message, severity='critical', title='❌ Pipeline Failed')
        self.send_email_alert(
            subject=f'Pipeline Failure: {task_name}',
            message=message,
            severity='critical'
        )
    
    # ============================================
    # DATA FRESHNESS ALERTS
    # ============================================
    
    def alert_stale_data(self, last_update, hours_old):
        """Alert for stale data"""
        
        message = f"""
🔄 DATA FRESHNESS ALERT

Last Update: {last_update}
Data Age: {hours_old} hours
Time: {datetime.now().isoformat()}

Action Required: Check if data pipeline is running.
"""
        
        severity = 'critical' if hours_old > 24 else 'warning'
        self.send_slack_alert(message, severity=severity, title=f'🔄 Data is {hours_old}h old')
        
        if hours_old > 24:
            self.send_email_alert(
                subject=f'Critical: Data is {hours_old} hours old',
                message=message,
                severity='critical'
            )
    
    # ============================================
    # DELAY ALERTS
    # ============================================
    
    def alert_high_delays(self, delayed_count, total_count):
        """Alert for high number of flight delays"""
        
        delay_percentage = (delayed_count / total_count * 100) if total_count > 0 else 0
        
        message = f"""
⚠️  HIGH DELAY ALERT

Delayed Flights: {delayed_count}/{total_count}
Delay Rate: {delay_percentage:.2f}%
Time: {datetime.now().isoformat()}

Recommendation: Review airlines and routes for investigation.
"""
        
        if delay_percentage > 20:  # Alert if >20% delayed
            self.send_slack_alert(message, severity='warning', title=f'⚠️  {delay_percentage:.1f}% Flights Delayed')
            self.send_email_alert(
                subject=f'High Flight Delay Rate: {delay_percentage:.1f}%',
                message=message,
                severity='warning'
            )

# ============================================
# ERROR HANDLING WRAPPER
# ============================================

class SafeAlertNotifier(AlertNotifier):
    """Alert notifier with comprehensive error handling"""
    
    def safe_send_alert(self, alert_type, **kwargs):
        """Safely send alerts with error handling"""
        
        try:
            if alert_type == 'quality_issues':
                return self.alert_quality_issues(**kwargs)
            elif alert_type == 'pipeline_success':
                return self.alert_pipeline_success(**kwargs)
            elif alert_type == 'pipeline_failure':
                return self.alert_pipeline_failure(**kwargs)
            elif alert_type == 'stale_data':
                return self.alert_stale_data(**kwargs)
            elif alert_type == 'high_delays':
                return self.alert_high_delays(**kwargs)
            else:
                print(f"❌ Unknown alert type: {alert_type}")
                return False
        
        except Exception as e:
            print(f"❌ Error sending alert: {str(e)}")
            print(f"   Alert type: {alert_type}")
            print(f"   Kwargs: {kwargs}")
            return False

# ============================================
# USAGE EXAMPLE
# ============================================

if __name__ == "__main__":
    # Configure alerts
    SLACK_WEBHOOK = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:ACCOUNT_ID:flight-alerts"
    
    alerter = SafeAlertNotifier(
        slack_webhook_url=SLACK_WEBHOOK,
        sns_topic_arn=SNS_TOPIC_ARN
    )
    
    # Example 1: Quality issues alert
    issues = [
        {'severity': 'CRITICAL', 'message': 'Missing required field: departure'},
        {'severity': 'WARNING', 'message': 'Found 15 duplicate records'}
    ]
    alerter.safe_send_alert('quality_issues', issues=issues)
    
    # Example 2: Success alert
    alerter.safe_send_alert('pipeline_success', task_name='data_quality_check', duration_seconds=45.5)
    
    # Example 3: Failure alert
    alerter.safe_send_alert('pipeline_failure', 
                           task_name='data_quality_check',
                           error_message='DynamoDB table not found',
                           error_type='ResourceNotFound')
