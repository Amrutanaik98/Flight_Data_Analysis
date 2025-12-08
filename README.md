# ✈️ Flight Data Analytics Pipeline

A production-grade, enterprise-scale data pipeline for real-time flight data ingestion, processing, analytics, and visualization. Built with AWS, Apache Airflow, Streamlit, Machine Learning, and advanced analytics.


---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Dashboard Visualizations](#dashboard-visualizations)
- [Machine Learning](#machine-learning)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Project Phases](#project-phases)
- [Monitoring](#monitoring)
- [Testing](#testing)

---

## Overview

This project builds a **complete data pipeline** that:

1. **Ingests** flight data from Aviation Stack API in real-time
2. **Processes** data using AWS Lambda (streaming) and AWS Glue (batch)
3. **Stores** data in multiple AWS services (S3, DynamoDB, SQS)
4. **Analyzes** flight patterns, delays, and performance metrics
5. **Predicts** flight delays using Machine Learning models (XGBoost, Random Forest)
6. **Visualizes** insights via Streamlit dashboard with 15+ interactive visualizations
7. **Orchestrates** everything using Apache Airflow on EC2
8. **Alerts** on anomalies and delays in real-time

**Perfect for:** Learning data engineering, portfolio projects, or production-ready systems.

---

## Architecture

```
FLIGHT DATA PIPELINE - Complete Data Flow
==========================================

PHASE 1: DATA INGESTION
┌──────────────┐
│ Aviation API │
└──────┬───────┘
       ▼
┌────────────────────────────┐
│   producer.py              │ (Fetches flight data)
└────────┬───────────────────┘
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌───────┐
│ S3     │ │ SQS   │ (Raw data storage)
│ (raw/) │ │ Queue │
└────────┘ └───┬───┘
              │
              ▼
          ┌─────────┐
          │ Lambda  │ (Processes messages)
          └────┬────┘
               │
               ▼
          ┌──────────────┐
          │ DynamoDB     │ (Real-time DB)
          │ (flights)    │
          └──────────────┘

BATCH PROCESSING
    │
    ▼ (S3 raw/)
┌────────────┐
│ AWS Glue   │ (Spark jobs)
└─────┬──────┘
      │
      ▼
  ┌─────────────┐
  │ S3          │
  │(processed/) │
  └─────────────┘

PHASE 2: ANALYTICS & VISUALIZATION
    │
    ├──────────────────────────────┐
    │                              │
    ▼                              ▼
┌──────────────┐      ┌──────────────────┐
│ Analytics    │      │ Streamlit        │
│ Engine       │      │ Dashboard        │
│              │      │ Port: 8055       │
│ - Airline    │      │                  │
│   Performance│      │ - Real-time KPIs │
│ - Routes     │      │ - Interactive    │
│ - Delays     │      │   Charts (15+)   │
│ - Trends     │      │ - Data Tables    │
└──────┬───────┘      └──────────────────┘
       │
       ▼
   ┌────────┐
   │ Reports│
   │ (S3)   │
   └────────┘

PHASE 3: MACHINE LEARNING
    │
    ▼
┌────────────────────────────┐
│ ML Models                  │
│ - XGBoost                  │
│ - Random Forest            │
│ - Feature Engineering      │
│ - Model Evaluation         │
│ - Delay Prediction         │
└────────┬───────────────────┘
         │
         ▼
    ┌──────────────┐
    │ Predictions  │
    │ (S3 + API)   │
    └──────────────┘

ORCHESTRATION & MONITORING
┌──────────────────────────────────────┐
│ Apache Airflow on EC2                │
│ Port: 8080                           │
│ - Runs daily                         │
│ - Monitors all tasks                 │
│ - Email alerts                       │
│ - Real-time monitoring               │
└──────────────────────────────────────┘
```

---

## Features

### Phase 1: Data Ingestion ✅
- ✅ Real-time flight data from Aviation Stack API
- ✅ Dual storage: S3 (raw) + SQS (queue)
- ✅ Lambda-based transformation
- ✅ DynamoDB for real-time queries
- ✅ AWS Glue batch processing

### Phase 2: Analytics & Reporting ✅
- ✅ Real-time Streamlit dashboard with 15+ KPIs
- ✅ Airline performance analysis
- ✅ Route optimization insights
- ✅ Delay pattern analysis
- ✅ Automated daily reports
- ✅ Historical trend analysis
- ✅ Interactive visualizations with Plotly & Pydeck

### Phase 3: Machine Learning ✅
- ✅ XGBoost delay prediction model
- ✅ Random Forest classifier
- ✅ Feature engineering pipeline
- ✅ Model training & evaluation
- ✅ Real-time delay forecasting
- ✅ Anomaly detection
- ✅ REST API endpoints for predictions



---

## Dashboard Visualizations

Your Streamlit dashboard includes:

- **Real-time KPIs:** Total flights, on-time rate, average delay
- **Airline Performance:** Comparison charts, ranking tables
- **Route Analytics:** Most delayed routes, busiest corridors
- **Delay Distribution:** Histograms, trend lines
- **Geographic Map:** Flight locations with Pydeck
- **Time-series Analysis:** Delays over time
- **Prediction Results:** ML model forecasts
- **Data Tables:** Detailed flight information
- **Filter Controls:** Dynamic filtering by airline, route, date
- **Alert Dashboard:** Real-time anomalies and warnings


<img width="1826" height="803" alt="d02" src="https://github.com/user-attachments/assets/493b84cc-33c2-4ff2-ba74-8d5bffe750a5" />
<img width="1803" height="827" alt="db03" src="https://github.com/user-attachments/assets/9e953901-7c38-47e7-82d7-7aa91dd15a9e" />
<img width="1872" height="272" alt="Screenshot 2025-12-07 160403" src="https://github.com/user-attachments/assets/82f6d71c-334c-4982-8a9b-9233be3a8715" />
<img width="1865" height="882" alt="Screenshot 2025-12-07 160519" src="https://github.com/user-attachments/assets/4dad5f8c-b211-4531-95e4-37c5eed71552" />
<img width="1862" height="567" alt="Screenshot 2025-12-07 160542" src="https://github.com/user-attachments/assets/57a352eb-af13-4d75-85c6-b8463fea8bde" />
<img width="1803" height="682" alt="Screenshot 2025-12-07 160606" src="https://github.com/user-attachments/assets/59351c34-ccba-49e2-b814-bc1933a89023" />





## Machine Learning

### Models Implemented

**XGBoost Regressor**
- Predicts flight delay duration (in minutes)
- Features: Departure airport, arrival airport, airline, aircraft type, time of day
- Accuracy: [Your accuracy metrics]
- Model evaluation metrics: MAE, RMSE, R² score

**Random Forest Classifier**
- Classifies flights as "On-Time" or "Delayed" (>15 minutes)
- Feature importance analysis
- Precision, Recall, F1-score: [Your metrics]

### Feature Engineering Pipeline
- Time-based features: Hour, day of week, month, seasonality
- Airport features: Historical delay patterns
- Airline features: Performance history
- Weather integration (when available)
- Traffic congestion indicators

### Model Performance
- Training/validation split: 80/20
- Cross-validation: 5-fold
- Hyperparameter tuning: GridSearchCV
- Regular retraining: Weekly schedule

### Prediction API
```bash
# Get delay prediction
curl -X POST http://34.195.227.103:5000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "departure_airport": "JFK",
    "arrival_airport": "LAX",
    "airline": "AA",
    "scheduled_time": "14:00"
  }'
```

---

## Tech Stack

### Cloud Platform
- **AWS:** EC2, S3, DynamoDB, SQS, Lambda, AWS Glue, CloudWatch, SNS

### Data Processing
- **Apache Airflow** - Workflow orchestration
- **AWS Glue/Spark** - Batch processing
- **Python 3.9+** - All scripts
- **Pandas** - Data manipulation
- **boto3** - AWS SDK

### Machine Learning
- **XGBoost** - Gradient boosting
- **Scikit-learn** - ML algorithms & preprocessing
- **Random Forest** - Classification
- **Joblib** - Model serialization
- **MLflow** - Experiment tracking (optional)

### Frontend & Visualization
- **Streamlit** - Real-time dashboard
- **Plotly** - Interactive charts
- **Pydeck** - Geospatial visualization
- **Flask** - REST API

### Development & Deployment
- **Git/GitHub** - Version control
- **Terraform** - Infrastructure as Code
- **AWS CLI** - Cloud management
- **Pytest** - Testing

---

## Getting Started

### Prerequisites
- AWS Account with free tier eligibility
- Python 3.9+ installed
- Git installed
- EC2 instance running Ubuntu 22.04 LTS
- SSH access to EC2

### Quick Start (10 minutes)

1. **Clone Repository**
```bash
git clone https://github.com/Amrutanaik98/flight-data-analysis.git
cd flight-data-analysis
```

2. **Create Virtual Environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install Dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure AWS**
```bash
aws configure
# Enter: AWS Access Key ID, Secret Key, Region (us-east-1)
```

5. **Deploy Infrastructure**
```bash
cd terraform/aws_resources
terraform init
terraform apply
```

6. **Deploy EC2 & Airflow**
```bash
cd ../airflow_ec2
terraform init
terraform apply
```

---

## Installation

### Local Development
```bash
git clone https://github.com/Amrutanaik98/flight-data-analysis.git
cd flight-data-analysis
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
aws configure
```

### Run Locally
```bash
# Run producer
python src/producer.py

# Run analytics
python analytics/flight_analytics.py

# Train ML models
python ml/train_models.py

# Run predictions
python ml/predictions.py

# Run REST API
python ml/api.py --port 5000

# Run dashboard
streamlit run dashboard/dashboard.py --server.port 8055
```

### AWS Deployment
```bash
# Deploy with Terraform
cd terraform/aws_resources
terraform init && terraform apply

cd ../airflow_ec2
terraform init && terraform apply

# SSH to EC2
ssh -i ~/.ssh/airflow-key.pem ubuntu@<PUBLIC_IP>

# Restart Airflow
pkill -f airflow
airflow scheduler > /tmp/scheduler.log 2>&1 &
airflow webserver --port 8080 > /tmp/webserver.log 2>&1 &
```

---

## Usage

### Run Pipeline

**Option 1: Trigger via CLI**
```bash
airflow dags trigger flight_data_pipeline
```

**Option 2: Trigger via Airflow UI**
1. Open http://34.195.227.103:8080
2. Find flight_data_pipeline
3. Click blue play button

**Option 3: Scheduled (Automatic)**
- Runs daily automatically
- Monitor via Airflow dashboard

### View Analytics

```bash
# Streamlit dashboard
streamlit run dashboard/dashboard.py --server.port 8055

# Run analytics manually
python analytics/flight_analytics.py

# Generate reports
python analytics/generate_reports.py

# View reports in S3
aws s3 ls s3://flights-data-lake-amruta/analytics/reports/
```

### Use Machine Learning Models

```bash
# Train models
python ml/train_models.py

# Make predictions
python ml/predictions.py --flight-id FL123

# Start prediction API
python ml/api.py --port 5000

# Check model metrics
python ml/evaluate_models.py
```

---

## Project Structure

```
flight-data-analysis/
│
├── src/
│   ├── producer.py              # Fetch API data
│   ├── producer_debug.py        # Testing
│   └── requirements.txt
│
├── analytics/
│   ├── flight_analytics.py      # Analytics engine
│   ├── generate_reports.py      # Report generation
│   ├── __init__.py
│   ├── reports/                 # Generated reports
│   └── data/                    # Processed data
│
├── ml/
│   ├── train_models.py          # Model training
│   ├── predictions.py           # Make predictions
│   ├── api.py                   # Flask REST API
│   ├── evaluate_models.py       # Model evaluation
│   ├── feature_engineering.py   # Feature pipeline
│   ├── models/                  # Trained models (joblib)
│   ├── data/                    # Training data
│   └── requirements.txt
│
├── dashboard/
│   ├── dashboard.py             # Main Streamlit app
│   ├── dashboard_test.py        # Test version
│   ├── requirements.txt
│   ├── run.sh                   # Start script
│   └── components/              # UI components
│
├── monitoring/
│   ├── health_monitor.py        # Health checks
│   ├── __init__.py
│   └── alert_manager.py         # Alerts
│
├── airflow/
│   ├── dags/
│   │   └── flight_data_dag.py  # Main DAG
│   ├── logs/                    # Execution logs
│   └── airflow.cfg              # Config
│
├── terraform/
│   ├── aws_resources/           # AWS setup
│   └── airflow_ec2/             # EC2 setup
│
├── tests/
│   ├── test_producer.py
│   ├── test_analytics.py
│   ├── test_ml_models.py
│   └── test_api.py
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DEPLOYMENT.md
│   ├── SETUP.md
│   └── ML_GUIDE.md
│
├── README.md
├── .gitignore
├── requirements.txt
└── screenshots/                 # Dashboard screenshots
```

---

## Data Flow

```
1. Producer fetches API data
   ↓
2. Data → S3 (raw/) + SQS
   ↓
3. Lambda triggered (external)
   ↓
4. DynamoDB updated (real-time)
   ↓
5. Glue job processes S3 (batch)
   ↓
6. S3 processed/ updated
   ↓
7. Analytics engine analyzes
   ↓
8. ML models make predictions
   ↓
9. Reports generated
   ↓
10. Dashboard updates
   ↓
11. Alerts sent (if needed)
   ↓
12. API serves predictions
```

---

## Project Phases

### ✅ Phase 1: Infrastructure (Complete)
- AWS resources deployed
- Terraform IaC ready
- EC2 with Airflow running
- Real-time data ingestion live

### ✅ Phase 2: Analytics & Reporting (Complete)
- Streamlit dashboard live with 15+ visualizations
- Daily analytics running
- Automated reports generated
- Interactive charts and KPIs

### ✅ Phase 3: Machine Learning (Complete)
- XGBoost delay prediction model
- Random Forest classifier
- Feature engineering pipeline
- Model evaluation & metrics
- REST API for predictions
- Real-time forecasting



---

## Monitoring

### Airflow Dashboard
- **Features:** Task status, logs, scheduling, retries

### Streamlit Dashboard
- **Features:** Real-time KPIs, charts, tables, trends, predictions




### CloudWatch Logs
```bash
# Lambda logs
aws logs tail /aws/lambda/flights-consumer-dev --follow

# Glue logs
aws logs tail /aws-glue/flights-job-dev --follow
```

---

## Testing

```bash
# Run all tests
pytest tests/

# Test producer
python src/producer_debug.py

# Test analytics
python analytics/flight_analytics.py

# Test ML models
python tests/test_ml_models.py

# Test API
python tests/test_api.py

# Test dashboard
streamlit run dashboard/dashboard_test.py
```

---




---

## Author

**Amruta Naik**
- GitHub: [@Amrutanaik98](https://github.com/Amrutanaik98)
- Project: [Flight Data Analysis](https://github.com/Amrutanaik98/flight-data-analysis)

---

## Support

For issues or questions:
1. Check [Documentation](docs/)
2. Search [GitHub Issues](https://github.com/Amrutanaik98/flight-data-analysis/issues)
3. Create new issue with details


---

## Project Statistics

- **Lines of Code:** 5,000+
- **AWS Services:** 8+
- **Phases:** 4 (3 complete, 1 in progress)
- **Analytics Metrics:** 20+
- **Dashboard Visualizations:** 15+
- **ML Models:** 2 (XGBoost, Random Forest)
- **API Endpoints:** 5+

---

**Last Updated:** December 7, 2025
