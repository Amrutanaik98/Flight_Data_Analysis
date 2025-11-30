# ============================================================
# MACHINE LEARNING: MAKE PREDICTIONS
# Purpose: Use trained model to predict flight delays
# ============================================================

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import joblib
import boto3
from io import BytesIO
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "flights-data-lake-amruta"
S3_MODEL_PATH = "ml/models/delay_model.pkl"
S3_PREDICTIONS_PATH = "ml/predictions/"

# ============================================================
# FUNCTION 1: LOAD MODEL FROM S3
# ============================================================

def load_model_from_s3():
    """Load trained model from S3"""
    logger.info("📥 Loading trained model from S3...")
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_MODEL_PATH)
        model = joblib.load(BytesIO(obj['Body'].read()))
        logger.info("✅ Model loaded successfully")
        return model
        
    except Exception as e:
        logger.error(f"❌ Error loading model: {e}")
        return None

# ============================================================
# FUNCTION 2: LOAD CURRENT FLIGHTS
# ============================================================

def load_current_flights_from_s3():
    """Load today's flight data from S3"""
    logger.info("📥 Loading current flights...")
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # List latest parquet file
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='processed/flights_main/',
            MaxKeys=1
        )
        
        if 'Contents' not in response:
            logger.warning("⚠️ No flights found")
            return None
        
        # Get the latest file
        latest_file = response['Contents'][0]['Key']
        logger.info(f"📖 Reading: {latest_file}")
        
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=latest_file)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        
        logger.info(f"✅ Loaded {len(df)} flights")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error loading flights: {e}")
        return None

# ============================================================
# FUNCTION 3: PREPARE FEATURES FOR PREDICTION
# ============================================================

def prepare_features_for_prediction(df):
    """Convert flight data to features (same as training)"""
    logger.info("🔨 Preparing features for prediction...")
    
    df = df.copy()
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Extract hour
    df['hour'] = df['timestamp'].dt.hour
    
    # Create time-of-day flags
    df['is_morning'] = ((df['hour'] >= 5) & (df['hour'] < 12)).astype(int)
    df['is_afternoon'] = ((df['hour'] >= 12) & (df['hour'] < 18)).astype(int)
    df['is_evening'] = ((df['hour'] >= 18) | (df['hour'] < 5)).astype(int)
    
    # Get day of week
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    
    # Is it weekend?
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    # Encode categorical variables
    airline_encoder = LabelEncoder()
    airline_encoder.fit(df['airline'].unique())
    df['airline_encoded'] = airline_encoder.transform(df['airline'].astype(str))
    
    # Create route
    df['route'] = df['departure'].astype(str) + ' → ' + df['arrival'].astype(str)
    route_encoder = LabelEncoder()
    route_encoder.fit(df['route'].unique())
    df['route_encoded'] = route_encoder.transform(df['route'].astype(str))
    
    # Select features for prediction
    feature_columns = [
        'hour',
        'is_morning',
        'is_afternoon',
        'is_evening',
        'is_weekend',
        'day_of_week',
        'airline_encoded',
        'route_encoded',
    ]
    
    X = df[feature_columns]
    
    # Return features and original data
    return X, df[['airline', 'departure', 'arrival', 'timestamp', 'status']]

# ============================================================
# FUNCTION 4: MAKE PREDICTIONS
# ============================================================

def make_predictions(model, X, original_data):
    """Use model to predict delays"""
    logger.info("🎯 Making predictions...")
    
    # Get predictions (0 or 1)
    predictions = model.predict(X)
    
    # Get probabilities (0.0 to 1.0)
    probabilities = model.predict_proba(X)
    
    # Get delay probability (class 1)
    delay_probability = probabilities[:, 1]
    
    # Combine results
    results = original_data.copy()
    results['prediction'] = predictions
    results['delay_probability'] = delay_probability
    
    # Determine risk level
    def get_risk_level(prob):
        if prob > 0.7:
            return "🔴 HIGH RISK"
        elif prob > 0.4:
            return "🟡 MEDIUM RISK"
        else:
            return "🟢 LOW RISK"
    
    results['risk_level'] = results['delay_probability'].apply(get_risk_level)
    
    logger.info(f"✅ Predictions completed for {len(results)} flights")
    
    return results

# ============================================================
# FUNCTION 5: FORMAT REPORT
# ============================================================

def format_predictions_report(predictions):
    """Format predictions into readable report"""
    logger.info("📝 Formatting report...")
    
    # Sort by delay probability (highest risk first)
    predictions_sorted = predictions.sort_values('delay_probability', ascending=False)
    
    # Build report
    report = []
    report.append("=" * 80)
    report.append("🚀 FLIGHT DELAY PREDICTIONS")
    report.append("=" * 80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total Flights: {len(predictions)}")
    report.append("")
    
    # Summary statistics
    high_risk = len(predictions[predictions['delay_probability'] > 0.7])
    medium_risk = len(predictions[(predictions['delay_probability'] > 0.4) & (predictions['delay_probability'] <= 0.7)])
    low_risk = len(predictions[predictions['delay_probability'] <= 0.4])
    
    report.append("📊 RISK BREAKDOWN:")
    report.append(f"  🔴 High Risk (>70%):   {high_risk} flights")
    report.append(f"  🟡 Medium Risk (40-70%): {medium_risk} flights")
    report.append(f"  🟢 Low Risk (<40%):    {low_risk} flights")
    report.append("")
    
    # Detailed predictions
    report.append("-" * 80)
    report.append("DETAILED PREDICTIONS:")
    report.append("-" * 80)
    
    for idx, row in predictions_sorted.iterrows():
        flight_info = f"{row['airline']} {row['departure']}→{row['arrival']} {row['timestamp'].strftime('%H:%M')}"
        probability_str = f"{row['delay_probability']:.1%}"
        report.append(f"{flight_info:50} | {row['risk_level']:15} | {probability_str:>6} chance")
    
    report.append("-" * 80)
    report.append("")
    
    # Recommendations
    report.append("💡 RECOMMENDATIONS:")
    if high_risk > 0:
        report.append(f"  - {high_risk} high-risk flights detected. Increase crew staffing.")
    if medium_risk > 0:
        report.append(f"  - Monitor {medium_risk} medium-risk flights closely.")
    if high_risk == 0 and medium_risk == 0:
        report.append("  - All flights low risk. Normal operations expected.")
    
    report.append("")
    report.append("=" * 80)
    
    report_text = "\n".join(report)
    return report_text

# ============================================================
# FUNCTION 6: SAVE PREDICTIONS TO S3
# ============================================================

def save_predictions_to_s3(predictions, report_text):
    """Save predictions and report to S3"""
    logger.info("💾 Saving predictions to S3...")
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Save CSV
        csv_buffer = BytesIO()
        predictions.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        csv_key = f"{S3_PREDICTIONS_PATH}predictions_{timestamp}.csv"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=csv_key,
            Body=csv_buffer.getvalue()
        )
        
        logger.info(f"✅ Predictions CSV saved: {csv_key}")
        
        # Save report
        report_key = f"{S3_PREDICTIONS_PATH}report_{timestamp}.txt"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=report_key,
            Body=report_text.encode('utf-8')
        )
        
        logger.info(f"✅ Report saved: {report_key}")
        
    except Exception as e:
        logger.error(f"❌ Error saving to S3: {e}")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main prediction pipeline"""
    logger.info("=" * 80)
    logger.info("🎯 MAKING FLIGHT DELAY PREDICTIONS")
    logger.info("=" * 80)
    
    try:
        # Step 1: Load model
        model = load_model_from_s3()
        if model is None:
            logger.error("❌ Could not load model")
            return
        
        # Step 2: Load current flights
        current_flights = load_current_flights_from_s3()
        if current_flights is None:
            logger.error("❌ Could not load flights")
            return
        
        # Step 3: Prepare features
        X, original_data = prepare_features_for_prediction(current_flights)
        
        # Step 4: Make predictions
        predictions = make_predictions(model, X, original_data)
        
        # Step 5: Format report
        report = format_predictions_report(predictions)
        
        # Step 6: Save to S3
        save_predictions_to_s3(predictions, report)
        
        # Step 7: Print report
        print("\n" + report + "\n")
        
        logger.info("=" * 80)
        logger.info("✅ PREDICTIONS COMPLETE!")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise

if __name__ == '__main__':
    main()
