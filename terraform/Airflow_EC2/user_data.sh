#!/bin/bash
set -e

echo "========================================="
echo "🚀 Setting up Airflow on EC2"
echo "========================================="

# Update system
sudo apt-get update
sudo apt-get upgrade -y

# Install dependencies
sudo apt-get install -y python3 python3-pip python3-venv git curl

# Create Airflow directory
mkdir -p ~/airflow_project
cd ~/airflow_project

# Create virtual environment
python3 -m venv airflow_env
source airflow_env/bin/activate

# Upgrade pip
pip install --upgrade pip setuptools wheel

# Install Airflow
pip install apache-airflow[amazon]==2.7.3 boto3 requests

# Initialize Airflow
export AIRFLOW_HOME=~/airflow_project/airflow
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@airflow.com \
    --password airflow123

# Create dags folder
mkdir -p ~/airflow_project/airflow/dags

# Create scheduler service
sudo bash -c 'cat > /etc/systemd/system/airflow-scheduler.service <<EOF
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/airflow_project
Environment="AIRFLOW_HOME=/home/ubuntu/airflow_project/airflow"
Environment="PATH=/home/ubuntu/airflow_project/airflow_env/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/home/ubuntu/airflow_project/airflow_env/bin/airflow scheduler
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF'

# Create webserver service
sudo bash -c 'cat > /etc/systemd/system/airflow-webserver.service <<EOF
[Unit]
Description=Airflow Webserver
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/airflow_project
Environment="AIRFLOW_HOME=/home/ubuntu/airflow_project/airflow"
Environment="PATH=/home/ubuntu/airflow_project/airflow_env/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/home/ubuntu/airflow_project/airflow_env/bin/airflow webserver --port 8080
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF'

# Start services
sudo systemctl daemon-reload
sudo systemctl enable airflow-scheduler airflow-webserver
sudo systemctl start airflow-scheduler airflow-webserver

# Permissions
sudo chown -R ubuntu:ubuntu ~/airflow_project

echo "========================================="
echo "✅ Airflow setup complete!"
echo "========================================="