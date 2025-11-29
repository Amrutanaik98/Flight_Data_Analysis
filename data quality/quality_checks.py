import boto3
import pandas as pd
from datetime import datetime
import json
from alert_system import SafeAlertNotifier

class DataQualityValidator:
    """Validate flight data quality with error handling"""
    
    def __init__(self, dynamodb_table_name='flights-realtime-dev', alerter=None):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.alerter = alerter
        self.quality_report = {
            'timestamp': datetime.now().isoformat(),
            'checks_passed': 0,
            'checks_failed': 0,
            'issues': []
        }
        
        try:
            self.table = self.dynamodb.Table(dynamodb_table_name)
        except Exception as e:
            print(f"❌ Error initializing DynamoDB: {e}")
            if alerter:
                alerter.safe_send_alert('pipeline_failure',
                                       task_name='data_quality_init',
                                       error_message=str(e),
                                       error_type='DynamoDBError')
            raise
    
    def check_completeness(self, df, threshold=95):
        """Check if required fields are not null"""
        required_fields = ['flight_id', 'airline', 'departure', 'arrival', 'status']
        
        print(f"\n🔍 Completeness Check")
        
        try:
            for field in required_fields:
                if field not in df.columns:
                    error_msg = f"Missing required field: {field}"
                    print(f"   ❌ {error_msg}")
                    self.quality_report['issues'].append({
                        'check': 'Completeness',
                        'severity': 'CRITICAL',
                        'message': error_msg
                    })
                    self.quality_report['checks_failed'] += 1
                    return False
                
                null_percentage = (df[field].isna().sum() / len(df)) * 100
                print(f"   ✅ {field}: {null_percentage:.2f}% null")
            
            self.quality_report['checks_passed'] += 1
            return True
        
        except Exception as e:
            error_msg = f"Error in completeness check: {str(e)}"
            print(f"   ❌ {error_msg}")
            self.quality_report['issues'].append({
                'check': 'Completeness',
                'severity': 'CRITICAL',
                'message': error_msg
            })
            self.quality_report['checks_failed'] += 1
            return False
    
    def check_uniqueness(self, df):
        """Check for duplicate records"""
        print(f"\n🔍 Uniqueness Check")
        
        try:
            total_records = len(df)
            unique_records = len(df.drop_duplicates(subset=['flight_id']))
            duplicates = total_records - unique_records
            duplicate_percentage = (duplicates / total_records * 100) if total_records > 0 else 0
            
            print(f"   ✅ Total: {total_records}, Unique: {unique_records}, Duplicates: {duplicates}")
            
            if duplicate_percentage > 5:
                warning_msg = f"High duplicates: {duplicate_percentage:.2f}%"
                print(f"   ⚠️  {warning_msg}")
                self.quality_report['issues'].append({
                    'check': 'Uniqueness',
                    'severity': 'WARNING',
                    'message': warning_msg
                })
            
            self.quality_report['checks_passed'] += 1
            return True
        
        except Exception as e:
            error_msg = f"Error in uniqueness check: {str(e)}"
            print(f"   ❌ {error_msg}")
            self.quality_report['issues'].append({
                'check': 'Uniqueness',
                'severity': 'WARNING',
                'message': error_msg
            })
            self.quality_report['checks_failed'] += 1
            return False
    
    def check_volume(self, df, min_records=10):
        """Check if data volume is within expected range"""
        print(f"\n🔍 Volume Check")
        
        try:
            record_count = len(df)
            print(f"   ✅ Records: {record_count}")
            
            if record_count < min_records:
                error_msg = f"Too few records: {record_count} (min: {min_records})"
                print(f"   ❌ {error_msg}")
                self.quality_report['issues'].append({
                    'check': 'Volume',
                    'severity': 'CRITICAL',
                    'message': error_msg
                })
                self.quality_report['checks_failed'] += 1
                return False
            
            self.quality_report['checks_passed'] += 1
            return True
        
        except Exception as e:
            error_msg = f"Error in volume check: {str(e)}"
            print(f"   ❌ {error_msg}")
            self.quality_report['issues'].append({
                'check': 'Volume',
                'severity': 'CRITICAL',
                'message': error_msg
            })
            self.quality_report['checks_failed'] += 1
            return False
    
    def run_all_checks(self, df):
        """Run all quality checks"""
        print("=" * 60)
        print("🔍 DATA QUALITY VALIDATION")
        print("=" * 60)
        
        try:
            self.check_completeness(df)
            self.check_uniqueness(df)
            self.check_volume(df)
            
            print("\n" + "=" * 60)
            print("✅ QUALITY CHECK SUMMARY")
            print("=" * 60)
            print(f"✅ Passed: {self.quality_report['checks_passed']}")
            print(f"❌ Failed: {self.quality_report['checks_failed']}")
            
            if self.quality_report['issues']:
                print(f"\n⚠️  Issues: {len(self.quality_report['issues'])}")
                for issue in self.quality_report['issues']:
                    print(f"   [{issue['severity']}] {issue['message']}")
                
                # Send alerts
                if self.alerter:
                    self.alerter.safe_send_alert('quality_issues', 
                                                issues=self.quality_report['issues'])
            else:
                print("\n✅ All checks passed!")
                if self.alerter:
                    self.alerter.safe_send_alert('pipeline_success',
                                                task_name='data_quality_check',
                                                duration_seconds=5)
            
            return self.quality_report
        
        except Exception as e:
            error_msg = f"Critical error in quality validation: {str(e)}"
            print(f"❌ {error_msg}")
            self.quality_report['issues'].append({
                'check': 'System',
                'severity': 'CRITICAL',
                'message': error_msg
            })
            
            if self.alerter:
                self.alerter.safe_send_alert('pipeline_failure',
                                            task_name='data_quality_validation',
                                            error_message=error_msg,
                                            error_type='ValidationError')
            
            return self.quality_report

# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    print("Testing Data Quality Checks with Alerts...\n")
    
    # Initialize alerter (optional)
    SLACK_WEBHOOK = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    alerter = SafeAlertNotifier(slack_webhook_url=SLACK_WEBHOOK)
    
    try:
        # Fetch from DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('flights-realtime-dev')
        
        response = table.scan()
        flights = response.get('Items', [])
        
        if not flights:
            print("❌ No data in DynamoDB!")
            alerter.safe_send_alert('pipeline_failure',
                                   task_name='data_fetch',
                                   error_message='No data found in DynamoDB',
                                   error_type='DataNotFound')
        else:
            df = pd.DataFrame(flights)
            validator = DataQualityValidator(alerter=alerter)
            report = validator.run_all_checks(df)
            
            # Save report
            with open('quality_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            print("\n📊 Report saved to: quality_report.json")
    
    except Exception as e:
        error_msg = f"Fatal error: {str(e)}"
        print(f"❌ {error_msg}")
        alerter.safe_send_alert('pipeline_failure',
                               task_name='quality_check_main',
                               error_message=error_msg,
                               error_type='UnexpectedError')
