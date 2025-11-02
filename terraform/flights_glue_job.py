import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, current_timestamp, count, countDistinct, when
from pyspark.sql.types import StructType, StructField, StringType
import json

# ============================================
# Glue Job Parameters
# ============================================

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"🚀 Starting Glue Job: {args['JOB_NAME']}")
print(f"📥 Input path: {args['S3_INPUT_PATH']}")
print(f"📤 Output path: {args['S3_OUTPUT_PATH']}")

try:
    # ============================================
    # Step 1: Read Raw Data from S3
    # ============================================
    
    print("\n📖 Step 1: Reading raw data from S3...")
    
    # Read JSON files with permissive mode to handle corrupt records
    raw_df = spark.read.option("mode", "PERMISSIVE").json(args['S3_INPUT_PATH'])
    
    total_records = raw_df.count()
    print(f"✅ Read {total_records} records")
    print(f"📊 Columns: {raw_df.columns}")
    
    # ============================================
    # Step 2: Handle Corrupt Records
    # ============================================
    
    print("\n🔧 Step 2: Handling corrupt records...")
    
    # Filter out corrupt records
    if '_corrupt_record' in raw_df.columns:
        corrupt_count = raw_df.filter(col('_corrupt_record').isNotNull()).count()
        print(f"⚠️  Found {corrupt_count} corrupt records")
        
        # Keep only non-corrupt records
        clean_df = raw_df.filter(col('_corrupt_record').isNull())
        clean_df = clean_df.drop('_corrupt_record')
        
        print(f"✅ Removed corrupt records: {clean_df.count()} valid records remain")
    else:
        clean_df = raw_df
        print(f"✅ No corrupt records found")
    
    # ============================================
    # Step 3: Data Cleaning
    # ============================================
    
    print("\n🧹 Step 3: Cleaning data...")
    
    # Check if flight_id exists, if not try alternate names
    available_cols = clean_df.columns
    print(f"📊 Available columns: {available_cols}")
    
    # Identify flight_id column (might be different name)
    flight_id_col = None
    if 'flight_id' in available_cols:
        flight_id_col = 'flight_id'
    elif 'flightId' in available_cols:
        flight_id_col = 'flightId'
    else:
        print(f"⚠️  flight_id column not found!")
        print(f"Available: {available_cols}")
        # Use first column as fallback
        flight_id_col = available_cols[0]
    
    print(f"✅ Using '{flight_id_col}' as flight identifier")
    
    # Remove duplicates
    cleaned_df = clean_df.dropDuplicates([flight_id_col, 'timestamp'])
    
    # Remove nulls for critical fields
    cleaned_df = cleaned_df.filter(col(flight_id_col).isNotNull())
    cleaned_df = cleaned_df.filter(col('airline').isNotNull())
    
    cleaned_count = cleaned_df.count()
    removed_count = total_records - cleaned_count
    
    print(f"✅ Cleaned data: {cleaned_count} records")
    print(f"📊 Removed: {removed_count} records")
    
    # ============================================
    # Step 4: Data Transformation
    # ============================================
    
    print("\n🔄 Step 4: Transforming data...")
    
    # Convert timestamp if it exists
    if 'timestamp' in cleaned_df.columns:
        transformed_df = cleaned_df.withColumn(
            'timestamp',
            to_timestamp(col('timestamp'))
        )
    else:
        transformed_df = cleaned_df
    
    # Add processing metadata
    transformed_df = transformed_df.withColumn(
        'processed_at',
        current_timestamp()
    )
    
    # Standardize column names to lowercase
    transformed_df = transformed_df.select(
        [col(c).alias(c.lower()) for c in transformed_df.columns]
    )
    
    # Rename flight_id column if it had different case
    flight_id_col_lower = flight_id_col.lower()
    if flight_id_col_lower != 'flight_id':
        transformed_df = transformed_df.withColumnRenamed(flight_id_col_lower, 'flight_id')
    
    print(f"✅ Transformation complete")
    print(f"📊 Final columns: {transformed_df.columns}")
    
    # ============================================
    # Step 5: Data Aggregations for Analytics
    # ============================================
    
    print("\n📊 Step 5: Creating aggregations for analytics...")
    
    # Aggregation 1: Flights by Airline
    airline_agg = (transformed_df
        .groupBy('airline')
        .agg(
            count('flight_id').alias('total_flights'),
            countDistinct('status').alias('status_types')
        )
        .select('airline', 'total_flights', 'status_types')
    )
    
    airline_count = airline_agg.count()
    print(f"✅ Airlines aggregated: {airline_count} airlines")
    
    # Aggregation 2: Flights by Status
    status_agg = (transformed_df
        .groupBy('status')
        .agg(count('flight_id').alias('flight_count'))
        .select('status', 'flight_count')
    )
    
    status_count = status_agg.count()
    print(f"✅ Status aggregated: {status_count} statuses")
    
    # Aggregation 3: Route Statistics
    route_agg = (transformed_df
        .groupBy('departure', 'arrival')
        .agg(count('flight_id').alias('flights_on_route'))
        .select('departure', 'arrival', 'flights_on_route')
    )
    
    route_count = route_agg.count()
    print(f"✅ Routes aggregated: {route_count} routes")
    
    # ============================================
    # Step 6: Write All Data to S3 Processed
    # ============================================
    
    print("\n💾 Step 6: Writing processed data to S3...")
    
    output_path = args['S3_OUTPUT_PATH']
    
    # Write 1: Cleaned & Transformed Main Data
    main_output = f"{output_path}/flights_main"
    transformed_df.write.mode("overwrite").parquet(main_output)
    print(f"✅ Written main data to: {main_output}")
    
    # Write 2: Airline Aggregation
    airline_output = f"{output_path}/flights_by_airline"
    airline_agg.write.mode("overwrite").parquet(airline_output)
    print(f"✅ Written airline aggregation to: {airline_output}")
    
    # Write 3: Status Aggregation
    status_output = f"{output_path}/flights_by_status"
    status_agg.write.mode("overwrite").parquet(status_output)
    print(f"✅ Written status aggregation to: {status_output}")
    
    # Write 4: Route Aggregation
    route_output = f"{output_path}/flights_by_route"
    route_agg.write.mode("overwrite").parquet(route_output)
    print(f"✅ Written route aggregation to: {route_output}")
    
    # ============================================
    # Step 7: Summary
    # ============================================
    
    print("\n" + "="*60)
    print("✅ GLUE JOB COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"📥 Input records: {total_records}")
    print(f"✅ Cleaned records: {cleaned_count}")
    print(f"❌ Removed records: {removed_count}")
    print(f"\n📊 AGGREGATIONS CREATED:")
    print(f"   • Airlines: {airline_count}")
    print(f"   • Flight Statuses: {status_count}")
    print(f"   • Routes: {route_count}")
    print(f"\n💾 All data written to: {output_path}")
    print(f"   Subfolders:")
    print(f"   • flights_main/")
    print(f"   • flights_by_airline/")
    print(f"   • flights_by_status/")
    print(f"   • flights_by_route/")
    print("="*60)
    
    job.commit()
    
except Exception as e:
    print(f"\n❌ ERROR in Glue Job: {str(e)}")
    print(f"Exception type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    job.commit()
    raise