import boto3
import pandas as pd
from datetime import datetime
import json

class DataQualityValidator:
    """Validate flight data quality"""
    
    def __init__(self, dynamodb_table_name='flights-realtime-dev'):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table(dynamodb_table_name)
        self.quality_report = {
            'timestamp': datetime.now().isoformat(),
            'checks_passed': 0,
            'checks_failed': 0,
            'issues': []
        }
    
    def check_completeness(self, df, threshold=95):
        """Check if required fields are not null"""
        required_fields = ['flight_id', 'airline', 'departure', 'arrival', 'status']
        
        print(f"\n🔍 Completeness Check")
        
        for field in required_fields:
            if field not in df.columns:
                print(f"   ❌ Missing field: {field}")
                self.quality_report['checks_failed'] += 1
                return False
            
            null_percentage = (df[field].isna().sum() / len(df)) * 100
            print(f"   ✅ {field}: {null_percentage:.2f}% null")
        
        self.quality_report['checks_passed'] += 1
        return True
    
    def check_uniqueness(self, df):
        """Check for duplicate records"""
        print(f"\n🔍 Uniqueness Check")
        
        total_records = len(df)
        unique_records = len(df.drop_duplicates(subset=['flight_id']))
        duplicates = total_records - unique_records
        
        print(f"   ✅ Total: {total_records}, Unique: {unique_records}, Duplicates: {duplicates}")
        
        self.quality_report['checks_passed'] += 1
        return True
    
    def check_volume(self, df, min_records=10):
        """Check if data volume is within expected range"""
        print(f"\n🔍 Volume Check")
        
        record_count = len(df)
        print(f"   ✅ Records: {record_count}")
        
        if record_count < min_records:
            print(f"   ❌ Too few records!")
            self.quality_report['checks_failed'] += 1
            return False
        
        self.quality_report['checks_passed'] += 1
        return True
    
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
            else:
                print("\n✅ All checks passed!")
            
            return self.quality_report
        
        except Exception as e:
            print(f"❌ Error: {e}")
            return self.quality_report

# Test it
if __name__ == "__main__":
    print("Testing Data Quality Checks...\n")
    
    # Fetch from DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('flights-realtime-dev')
    
    try:
        response = table.scan()
        flights = response.get('Items', [])
        
        if not flights:
            print("❌ No data in DynamoDB!")
        else:
            df = pd.DataFrame(flights)
            validator = DataQualityValidator()
            report = validator.run_all_checks(df)
            
            # Save report
            with open('quality_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            print("\n📊 Report saved to: quality_report.json")
    
    except Exception as e:
        print(f"❌ Error connecting to DynamoDB: {e}")

