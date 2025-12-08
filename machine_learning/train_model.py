#!/usr/bin/env python3
# ============================================================
# MACHINE LEARNING: TRAIN MODEL (FIXED)
# Purpose: Train ML model with proper encoder handling
# ============================================================

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import joblib
import boto3
from io import BytesIO
import logging
import pickle

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "flights-data-lake-amruta"
S3_MODEL_PATH = "ml/models/delay_model.pkl"
S3_ENCODERS_PATH = "ml/models/encoders.pkl"
TEST_SIZE = 0.2
RANDOM_STATE = 42

# ============================================================
# FUNCTION 1: LOAD DATA FROM S3
# ============================================================

def load_data_from_s3():
    """Load flight data from S3 Parquet files"""
    logger.info("📥 Loading data from S3...")
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # List parquet files in processed/flights_main
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='processed/flights_main/',
            MaxKeys=100
        )
        
        # Check if files exist
        if 'Contents' not in response:
            logger.error("❌ No files found in S3 processed/flights_main/")
            return None
        
        # Get list of parquet files
        parquet_files = [
            obj['Key'] 
            for obj in response['Contents'] 
            if obj['Key'].endswith('.parquet')
        ]
        
        logger.info(f"✅ Found {len(parquet_files)} parquet files")
        
        if len(parquet_files) == 0:
            logger.error("❌ No parquet files found")
            return None
        
        # Load all files and combine
        dfs = []
        for parquet_file in parquet_files:
            try:
                logger.info(f"📖 Reading: {parquet_file}")
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=parquet_file)
                df = pd.read_parquet(BytesIO(obj['Body'].read()))
                dfs.append(df)
                logger.info(f"   ✅ Loaded {len(df)} records")
            except Exception as e:
                logger.warning(f"⚠️ Error reading {parquet_file}: {e}")
                continue
        
        if not dfs:
            logger.error("❌ No data could be read from any file")
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"✅ Total loaded: {len(combined_df)} records")
        logger.info(f"📊 Columns: {list(combined_df.columns)}")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# ============================================================
# FUNCTION 2: CREATE FEATURES & ENCODERS
# ============================================================

def create_features(df):
    """
    Convert raw data into features for ML model
    Returns: X, y, and encoders dict (for later reuse)
    """
    logger.info("🔨 Creating features...")
    
    df = df.copy()
    encoders = {}
    
    try:
        # Convert time to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract hour from timestamp
        df['hour'] = df['timestamp'].dt.hour
        
        # Create time-of-day flags
        df['is_morning'] = ((df['hour'] >= 5) & (df['hour'] < 12)).astype(int)
        df['is_afternoon'] = ((df['hour'] >= 12) & (df['hour'] < 18)).astype(int)
        df['is_evening'] = ((df['hour'] >= 18) | (df['hour'] < 5)).astype(int)
        
        # Get day of week
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        # Is it weekend?
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # ✅ FIXED: Encode categorical variables and SAVE encoders
        logger.info("  Encoding airline...")
        airline_encoder = LabelEncoder()
        df['airline'] = df['airline'].fillna('UNKNOWN').astype(str)
        df['airline_encoded'] = airline_encoder.fit_transform(df['airline'])
        encoders['airline'] = airline_encoder
        logger.info(f"    • Unique airlines: {len(airline_encoder.classes_)}")
        
        # Create route from departure and arrival
        df['departure'] = df['departure'].fillna('UNKNOWN').astype(str)
        df['arrival'] = df['arrival'].fillna('UNKNOWN').astype(str)
        df['route'] = df['departure'] + ' → ' + df['arrival']
        
        logger.info("  Encoding route...")
        route_encoder = LabelEncoder()
        df['route_encoded'] = route_encoder.fit_transform(df['route'])
        encoders['route'] = route_encoder
        logger.info(f"    • Unique routes: {len(route_encoder.classes_)}")
        
        # Create target variable
        df['status'] = df['status'].fillna('unknown').astype(str)
        df['is_delayed'] = (df['status'].isin(['delayed', 'active'])).astype(int)
        
        # Select features for model
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
        y = df['is_delayed']
        
        logger.info(f"✅ Created features: {len(X)} records × {len(feature_columns)} features")
        logger.info(f"📊 Target distribution:")
        logger.info(f"   • Delayed (1): {(y == 1).sum()} ({(y == 1).sum()/len(y):.1%})")
        logger.info(f"   • Not delayed (0): {(y == 0).sum()} ({(y == 0).sum()/len(y):.1%})")
        
        return X, y, encoders
        
    except Exception as e:
        logger.error(f"❌ Error creating features: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None, None

# ============================================================
# FUNCTION 3: TRAIN MODEL
# ============================================================

def train_model(X, y):
    """Train Random Forest model"""
    logger.info("🤖 Training ML model...")
    
    try:
        # Split data: 80% training, 20% testing
        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=TEST_SIZE,
            random_state=RANDOM_STATE
        )
        
        logger.info(f"📊 Training data: {len(X_train)} records")
        logger.info(f"📊 Testing data: {len(X_test)} records")
        
        # Create and train model
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=RANDOM_STATE,
            n_jobs=-1,
            verbose=1
        )
        
        # Train the model
        logger.info("  Training started...")
        model.fit(X_train, y_train)
        logger.info("✅ Model training completed")
        
        # Check accuracy
        train_accuracy = model.score(X_train, y_train)
        test_accuracy = model.score(X_test, y_test)
        
        logger.info(f"🎯 Training accuracy: {train_accuracy:.2%}")
        logger.info(f"🎯 Testing accuracy: {test_accuracy:.2%}")
        
        # Show feature importance
        feature_importance = pd.DataFrame({
            'feature': X.columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info("📊 Feature Importance:")
        for idx, row in feature_importance.iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.2%}")
        
        return model
        
    except Exception as e:
        logger.error(f"❌ Error training model: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# ============================================================
# FUNCTION 4: SAVE MODEL & ENCODERS TO S3
# ============================================================

def save_model_to_s3(model, encoders):
    """Save trained model AND encoders to S3"""
    logger.info("💾 Saving model and encoders to S3...")
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # ✅ FIXED: Save model
        logger.info("  Saving model...")
        model_bytes = BytesIO()
        joblib.dump(model, model_bytes)
        model_bytes.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_MODEL_PATH,
            Body=model_bytes.getvalue()
        )
        logger.info(f"✅ Model saved: {S3_MODEL_PATH}")
        
        # ✅ FIXED: Save encoders
        logger.info("  Saving encoders...")
        encoders_bytes = BytesIO()
        pickle.dump(encoders, encoders_bytes)
        encoders_bytes.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_ENCODERS_PATH,
            Body=encoders_bytes.getvalue()
        )
        logger.info(f"✅ Encoders saved: {S3_ENCODERS_PATH}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving to S3: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main training pipeline"""
    logger.info("=" * 70)
    logger.info("🚀 MACHINE LEARNING: TRAINING MODEL")
    logger.info("=" * 70)
    logger.info("")
    
    try:
        # Step 1: Load data
        logger.info("STEP 1: Load Data")
        df = load_data_from_s3()
        if df is None:
            logger.error("❌ Failed to load data - exiting")
            return False
        logger.info("")
        
        # Step 2: Create features
        logger.info("STEP 2: Create Features")
        X, y, encoders = create_features(df)
        if X is None:
            logger.error("❌ Failed to create features - exiting")
            return False
        logger.info("")
        
        # Step 3: Train model
        logger.info("STEP 3: Train Model")
        model = train_model(X, y)
        if model is None:
            logger.error("❌ Failed to train model - exiting")
            return False
        logger.info("")
        
        # Step 4: Save model and encoders
        logger.info("STEP 4: Save Model & Encoders")
        success = save_model_to_s3(model, encoders)
        if not success:
            logger.error("❌ Failed to save model - exiting")
            return False
        logger.info("")
        
        logger.info("=" * 70)
        logger.info("✅ MODEL TRAINING COMPLETE!")
        logger.info("=" * 70)
        logger.info("")
        logger.info("✅ Model saved to: s3://{}/{}".format(S3_BUCKET, S3_MODEL_PATH))
        logger.info("✅ Encoders saved to: s3://{}/{}".format(S3_BUCKET, S3_ENCODERS_PATH))
        logger.info("")
        
        return True
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error("❌ FATAL ERROR")
        logger.error("=" * 70)
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
