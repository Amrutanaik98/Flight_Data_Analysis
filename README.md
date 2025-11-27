# ✈️ Flight Data Analytics Pipeline

A production-grade, enterprise-scale data pipeline for real-time flight data ingestion, processing, analytics, and visualization. Built with AWS, Apache Airflow, Streamlit, and Machine Learning.

**Status:** 🟢 Active Development | Phase 2 Complete | Phase 3 In Progress

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Phases](#phases)
- [Monitoring](#monitoring)
- [Contributing](#contributing)

---

## 🎯 Overview

This project builds a **complete data pipeline** that:

1. **Ingests** flight data from Aviation Stack API in real-time
2. **Processes** data using AWS Lambda (streaming) and AWS Glue (batch)
3. **Stores** data in multiple AWS services (S3, DynamoDB, SQS)
4. **Analyzes** flight patterns, delays, and performance metrics
5. **Visualizes** insights via Streamlit dashboard
6. **Orchestrates** everything using Apache Airflow on EC2
7. **Alerts** on anomalies and delays in real-time

**Perfect for:** Learning data engineering, portfolio projects, or production-ready systems.

---

## 🏗️ Architecture

```
FLIGHT DATA PIPELINE - Data Flow
================================

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
    ├─────────────────────────┐
    │                         │
    ▼                         ▼
┌──────────────┐      ┌──────────────────┐
│ Analytics    │      │ Streamlit        │
│ Engine       │      │ Dashboard        │
│              │      │ Port: 8055       │
│ - Airline    │      │                  │
│   Performance│      │ - Real-time KPIs │
│ - Routes     │      │ - Interactive    │
│ - Delays     │      │   Charts         │
│ - Trends     │      │ - Data Tables    │
└──────┬───────┘      └──────────────────┘
       │
       ▼
   ┌────────┐
   │ Reports│
   │ (S3)   │
   └────────┘

ORCHESTRATION
┌──────────────────────────────────────┐
│ Apache Airflow on EC2                │
│ Port: 8080                           │
│ - Runs daily                         │
│ - Monitors all tasks                 │
│ - Email alerts                       │
└──────────────────────────────────────┘
```

---

## ✨ Features

### Phase 1: Data Ingestion ✅
- ✅ Real-time flight data from Aviation Stack API
- ✅ Dual storage: S3 (raw) + SQS (queue)
- ✅ Lambda-based transformation
- ✅ DynamoDB for real-time queries
- ✅ AWS Glue batch processing

### Phase 2: Analytics & Reporting ✅
- ✅ Real-time Streamlit dashboard with KPIs
- ✅ Airline performance analysis
- ✅ Route optimization insights
- ✅ Delay pattern analysis
- ✅ Automated daily reports
- ✅ Historical trend analysis

### Phase 3: Advanced Analytics 🚧
- 🔄 Machine Learning (delay prediction)
- 🔄 Real-time alerts
- 🔄 REST API
- 🔄 Anomaly detection

### Phase 4: Infrastructure 📅
- 📋 Data warehouse (Redshift/BigQuery)
- 📋 Mobile app (React Native/Flutter)
- 📋 Web portal (React.js)

### Phase 5: Enterprise 📅
- 📋 Docker containerization
- 📋 Kubernetes deployment
- 📋 Security & compliance
- 📋 Multi-source integration

---

## 🛠️ Tech Stack

### Cloud Platform
- **AWS:** EC2, S3, DynamoDB, SQS, Lambda, AWS Glue, CloudWatch, SNS

### Data Processing
- **Apache Airflow** - Workflow orchestration
- **AWS Glue/Spark** - Batch processing
- **Python 3.9+** - All scripts
- **Pandas** - Data manipulation
- **boto3** - AWS SDK

### Frontend & Visualization
- **Streamlit** - Real-time dashboard
- **Plotly** - Interactive charts
- **Pydeck** - Geospatial visualization

### Development & Deployment
- **Git/GitHub** - Version control
- **Terraform** - Infrastructure as Code
- **Docker** - Containerization (Phase 5)
- **Kubernetes** - Orchestration (Phase 5)

---

## 🚀 Getting Started

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

## 📦 Installation

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

## 💻 Usage

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

---

## 📁 Project Structure

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
│   └── test_api.py
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DEPLOYMENT.md
│   └── SETUP.md
│
├── README.md
├── .gitignore
├── requirements.txt
└── Dockerfile
```

---

## 📊 Data Flow

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
8. Reports generated
   ↓
9. Dashboard updates
   ↓
10. Alerts sent (if needed)
```

---

## 📊 Phases

### ✅ Phase 1: Infrastructure (Complete)
- AWS resources deployed
- Terraform IaC ready
- EC2 with Airflow running

### ✅ Phase 2: Analytics & Reporting (Complete)
- Streamlit dashboard live
- Daily analytics running
- Automated reports generated

### 🔄 Phase 3: Advanced Analytics (In Progress)
- ML delay prediction model
- Real-time alert system
- REST API endpoints

### 📋 Phase 4: Data Infrastructure (Planned)
- Data warehouse setup
- Mobile app development
- Web portal

### 📋 Phase 5: Enterprise (Planned)
- Docker containerization
- Kubernetes deployment
- Security & compliance

---

## 📈 Monitoring

### Airflow Dashboard
- **URL:** http://34.195.227.103:8080
- **Features:** Task status, logs, scheduling, retries

### Streamlit Dashboard
- **URL:** http://34.195.227.103:8055
- **Features:** Real-time KPIs, charts, tables, trends

### CloudWatch Logs
```bash
# Lambda logs
aws logs tail /aws/lambda/flights-consumer-dev --follow

# Glue logs
aws logs tail /aws-glue/flights-job-dev --follow
```

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Test producer
python src/producer_debug.py

# Test analytics
python analytics/flight_analytics.py

# Test dashboard
streamlit run dashboard/dashboard_test.py
```

---

## 🤝 Contributing

1. Fork repository
2. Create feature branch (`git checkout -b feature/YourFeature`)
3. Commit changes (`git commit -m 'Add YourFeature'`)
4. Push to branch (`git push origin feature/YourFeature`)
5. Open Pull Request

---

## 📝 License

MIT License - See LICENSE file for details

---

## 👩‍💼 Author

**Amruta Naik**
- GitHub: [@Amrutanaik98](https://github.com/Amrutanaik98)
- Project: [Flight Data Analysis](https://github.com/Amrutanaik98/flight-data-analysis)

---

## 💬 Support

For issues or questions:
1. Check [Documentation](docs/)
2. Search [GitHub Issues](https://github.com/Amrutanaik98/flight-data-analysis/issues)
3. Create new issue with details

---

## 🎯 Next Steps

1. ✅ Phase 2 Analytics
2. 🔄 Phase 3 Machine Learning
3. 📋 Phase 4 Data Infrastructure
4. 🐳 Phase 5 Docker/Kubernetes

---

## 📊 Project Statistics

- **Lines of Code:** 3,500+
- **AWS Services:** 8+
- **Phases:** 5 (2 complete, 3 planned)
- **Analytics Metrics:** 20+
- **Dashboard Visualizations:** 15+

---

**Last Updated:** November 26, 2025
**Status:** 🟢 Active Development
**Next Phase:** Phase 3 - Machine Learning
