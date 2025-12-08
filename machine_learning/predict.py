#!/usr/bin/env python3
# ============================================================
# MACHINE LEARNING: MAKE PREDICTIONS (FIXED)
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
import pickle

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "flights-data-lake-amruta"
S3_MODEL_PATH = "ml/models/delay_model.pkl"
S3_ENCODERS_PATH = "ml/models/encoders.pkl"
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
        import traceback
        logger.error(traceback.format_exc())
        return None

# ============================================================
# FUNCTION 2: LOAD ENCODERS FROM S3 (NEW!)
# ============================================================

def load_encoders_from_s3():
    """✅ FIXED: Load encoders from S3"""
    logger.info("📥 Loading encoders from S3...")
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_ENCODERS_PATH)
        encoders = pickle.load(BytesIO(obj['Body'].read()))
        logger.info("✅ Encoders loaded successfully")
        logger.info(f"   • Airline encoder: {len(encoders['airline'].classes_)} classes")
        logger.info(f"   • Route encoder: {len(encoders['route'].classes_)} classes")
        return encoders
        
    except Exception as e:
        logger.error(f"❌ Error loading encoders: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# ============================================================
# FUNCTION 3: LOAD CURRENT FLIGHTS
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
            MaxKeys=100
        )
        
        if 'Contents' not in response:
            logger.warning("⚠️ No flights found")
            return None
        
        # Get all files
        parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        
        if not parquet_files:
            logger.error("❌ No parquet files found")
            return None
        
        logger.info(f"✅ Found {len(parquet_files)} parquet files")
        
        # Load all files
        dfs = []
        for parquet_file in parquet_files:
            try:
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=parquet_file)
                df = pd.read_parquet(BytesIO(obj['Body'].read()))
                dfs.append(df)
            except Exception as e:
                logger.warning(f"⚠️ Error reading {parquet_file}: {e}")
                continue
        
        if not dfs:
            logger.error("❌ No data could be loaded")
            return None
        
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"✅ Loaded {len(combined_df)} flights")
        return combined_df
        
    except Exception as e:
        logger.error(f"❌ Error loading flights: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# ============================================================
# FUNCTION 4: PREPARE FEATURES FOR PREDICTION (FIXED!)
# ============================================================

def prepare_features_for_prediction(df, encoders):
    """✅ FIXED: Convert flight data to features using saved encoders"""
    logger.info("🔨 Preparing features for prediction...")
    
    try:
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
        
        # ✅ FIXED: Use saved airline encoder
        logger.info("  Encoding airline...")
        df['airline'] = df['airline'].fillna('UNKNOWN').astype(str)
        airline_encoder = encoders['airline']
        
        # Handle unseen airlines
        df['airline_encoded'] = df['airline'].apply(
            lambda x: airline_encoder.transform([x])[0] if x in airline_encoder.classes_ 
            else airline_encoder.transform(['UNKNOWN'])[0]
        )
        
        # ✅ FIXED: Use saved route encoder
        logger.info("  Encoding route...")
        df['departure'] = df['departure'].fillna('UNKNOWN').astype(str)
        df['arrival'] = df['arrival'].fillna('UNKNOWN').astype(str)
        df['route'] = df['departure'] + ' → ' + df['arrival']
        
        route_encoder = encoders['route']
        
        # Handle unseen routes
        df['route_encoded'] = df['route'].apply(
            lambda x: route_encoder.transform([x])[0] if x in route_encoder.classes_
            else route_encoder.transform([df['route'].iloc[0]])[0]
        )
        
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
        
        X = df[feature_columns].fillna(0)
        
        # Return features and original data
        logger.info(f"✅ Prepared {len(X)} flights for prediction")
        return X, df[['airline', 'departure', 'arrival', 'timestamp', 'status']]
        
    except Exception as e:
        logger.error(f"❌ Error preparing features: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None

# ============================================================
# FUNCTION 5: MAKE PREDICTIONS
# ============================================================

def make_predictions(model, X, original_data):
    """Use model to predict delays"""
    logger.info("🎯 Making predictions...")
    
    try:
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
        
    except Exception as e:
        logger.error(f"❌ Error making predictions: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# ============================================================
# FUNCTION 6: FORMAT REPORT
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
    report.append(f"  🔴 High Risk (>70%):     {high_risk} flights")
    report.append(f"  🟡 Medium Risk (40-70%): {medium_risk} flights")
    report.append(f"  🟢 Low Risk (<40%):      {low_risk} flights")
    report.append("")
    
    # Detailed predictions (top 20)
    report.append("-" * 80)
    report.append("TOP 20 HIGHEST RISK FLIGHTS:")
    report.append("-" * 80)
    
    for idx, (i, row) in enumerate(predictions_sorted.head(20).iterrows()):
        flight_info = f"{row['airline']} {row['departure']}→{row['arrival']} {row['timestamp'].strftime('%H:%M')}"
        probability_str = f"{row['delay_probability']:.1%}"
        report.append(f"{flight_info:50} | {row['risk_level']:15} | {probability_str:>6}")
    
    report.append("-" * 80)
    report.append("")
    
    # Recommendations
    report.append("💡 RECOMMENDATIONS:")
    if high_risk > 0:
        report.append(f"  • {high_risk} high-risk flights detected. Increase crew staffing.")
    if medium_risk > 0:
        report.append(f"  • Monitor {medium_risk} medium-risk flights closely.")
    if high_risk == 0 and medium_risk == 0:
        report.append("  • All flights low risk. Normal operations expected.")
    
    report.append("")
    report.append("=" * 80)
    
    report_text = "\n".join(report)
    return report_text

# ============================================================
# FUNCTION 7: SAVE PREDICTIONS TO S3
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
        import traceback
        logger.error(traceback.format_exc())

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main prediction pipeline"""
    logger.info("=" * 80)
    logger.info("🎯 MAKING FLIGHT DELAY PREDICTIONS")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        # Step 1: Load model
        logger.info("STEP 1: Load Model")
        model = load_model_from_s3()
        if model is None:
            logger.error("❌ Could not load model")
            return False
        logger.info("")
        
        # Step 2: Load encoders (NEW!)
        logger.info("STEP 2: Load Encoders")
        encoders = load_encoders_from_s3()
        if encoders is None:
            logger.error("❌ Could not load encoders")
            return False
        logger.info("")
        
        # Step 3: Load current flights
        logger.info("STEP 3: Load Current Flights")
        current_flights = load_current_flights_from_s3()
        if current_flights is None:
            logger.error("❌ Could not load flights")
            return False
        logger.info("")
        
        # Step 4: Prepare features
        logger.info("STEP 4: Prepare Features")
        X, original_data = prepare_features_for_prediction(current_flights, encoders)
        if X is None:
            logger.error("❌ Could not prepare features")
            return False
        logger.info("")
        
        # Step 5: Make predictions
        logger.info("STEP 5: Make Predictions")
        predictions = make_predictions(model, X, original_data)
        if predictions is None:
            logger.error("❌ Could not make predictions")
            return False
        logger.info("")
        
        # Step 6: Format report
        logger.info("STEP 6: Format Report")
        report = format_predictions_report(predictions)
        logger.info("")
        
        # Step 7: Save to S3
        logger.info("STEP 7: Save Predictions")
        save_predictions_to_s3(predictions, report)
        logger.info("")
        
        # Step 8: Print report
        print("\n" + report + "\n")
        
        logger.info("=" * 80)
        logger.info("✅ PREDICTIONS COMPLETE!")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ FATAL ERROR")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
