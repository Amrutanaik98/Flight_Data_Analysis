# ============================================================
# MACHINE LEARNING: TRAIN MODEL
# Purpose: Train ML model to predict flight delays
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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "flights-data-lake-amruta"
S3_MODEL_PATH = "ml/models/delay_model.pkl"
TEST_SIZE = 0.2
RANDOM_STATE = 42

# ============================================================
# FUNCTION 1: LOAD DATA FROM S3
# ============================================================

def load_data_from_s3():
    """Load flight data from S3 Parquet files"""
    logger.info("📥 Loading data from S3...")
    
    try:
        # Create S3 client
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # List parquet files in processed/flights_main
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='processed/flights_main/',
            MaxKeys=100
        )
        
        # Check if files exist
        if 'Contents' not in response:
            logger.error("❌ No files found in S3")
            return None
        
        # Get list of parquet files
        parquet_files = [
            obj['Key'] 
            for obj in response['Contents'] 
            if obj['Key'].endswith('.parquet')
        ]
        
        logger.info(f"✅ Found {len(parquet_files)} parquet files")
        
        # Load all files and combine
        dfs = []
        for parquet_file in parquet_files:
            try:
                logger.info(f"📖 Reading: {parquet_file}")
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=parquet_file)
                df = pd.read_parquet(BytesIO(obj['Body'].read()))
                dfs.append(df)
            except Exception as e:
                logger.warning(f"⚠️ Error reading {parquet_file}: {e}")
                continue
        
        if not dfs:
            logger.error("❌ No data could be read")
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"✅ Loaded {len(combined_df)} total records")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}")
        return None

# ============================================================
# FUNCTION 2: CREATE FEATURES
# ============================================================

def create_features(df):
    """
    Convert raw data into features for ML model
    Features = inputs for the model
    Target = what we want to predict (delay yes/no)
    """
    logger.info("🔨 Creating features...")
    
    df = df.copy()
    
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
    
    # Encode categorical variables (convert text to numbers)
    airline_encoder = LabelEncoder()
    df['airline_encoded'] = airline_encoder.fit_transform(df['airline'].astype(str))
    
    # Create route from departure and arrival
    df['route'] = df['departure'].astype(str) + ' → ' + df['arrival'].astype(str)
    route_encoder = LabelEncoder()
    df['route_encoded'] = route_encoder.fit_transform(df['route'].astype(str))
    
    # Create target variable (what we want to predict)
    # Check if flight is active/delayed/scheduled
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
    
    X = df[feature_columns]
    y = df['is_delayed']
    
    logger.info(f"✅ Created {len(X)} records with {len(feature_columns)} features")
    
    return X, y

# ============================================================
# FUNCTION 3: TRAIN MODEL
# ============================================================

def train_model(X, y):
    """Train Random Forest model"""
    logger.info("🤖 Training ML model...")
    
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
        n_jobs=-1
    )
    
    # Train the model
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

# ============================================================
# FUNCTION 4: SAVE MODEL TO S3
# ============================================================

def save_model_to_s3(model):
    """Save trained model to S3"""
    logger.info("💾 Saving model to S3...")
    
    try:
        # Convert model to bytes
        model_bytes = BytesIO()
        joblib.dump(model, model_bytes)
        model_bytes.seek(0)
        
        # Upload to S3
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_MODEL_PATH,
            Body=model_bytes.getvalue()
        )
        
        logger.info(f"✅ Model saved to S3: {S3_MODEL_PATH}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving model: {e}")
        return False

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main training pipeline"""
    logger.info("=" * 60)
    logger.info("🚀 MACHINE LEARNING: TRAINING MODEL")
    logger.info("=" * 60)
    
    try:
        # Step 1: Load data
        df = load_data_from_s3()
        if df is None:
            logger.error("❌ Failed to load data")
            return
        
        # Step 2: Create features
        X, y = create_features(df)
        
        # Step 3: Train model
        model = train_model(X, y)
        
        # Step 4: Save model
        save_model_to_s3(model)
        
        logger.info("=" * 60)
        logger.info("✅ MODEL TRAINING COMPLETE!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise

if __name__ == '__main__':
    main()
