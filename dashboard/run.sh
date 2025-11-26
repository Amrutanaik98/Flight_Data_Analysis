#!/bin/bash

echo "🚀 Starting Flight Data Analytics Dashboard..."
echo "================================================"

# Navigate to project root
cd /home/ubuntu/Flight_Data_Analysis

# Activate virtual environment
source venv/bin/activate

# Run dashboard
streamlit run dashboard/dashboard.py --server.port 8501

