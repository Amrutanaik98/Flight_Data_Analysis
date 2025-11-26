import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Flight Dashboard Test", layout="wide")

st.title("✈️ Flight Data Analytics Dashboard")
st.markdown("Test Dashboard with Sample Data")

# Create sample data (no AWS connection needed)
df = pd.DataFrame({
    'flight_id': ['FL001', 'FL002', 'FL003', 'FL004', 'FL005'],
    'airline': ['Delta', 'United', 'American', 'Delta', 'Southwest'],
    'departure': ['NYC', 'LAX', 'ORD', 'DFW', 'SFO'],
    'arrival': ['LAX', 'NYC', 'MIA', 'LAX', 'DEN'],
    'status': ['on time', 'delayed', 'on time', 'cancelled', 'delayed'],
    'delay_minutes': [0, 25, 0, 0, 15]
})

# Display metrics
st.markdown("### 📈 Key Metrics")
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Flights", len(df))
col2.metric("On-Time", len(df[df['status'] == 'on time']))
col3.metric("Delayed", len(df[df['status'] == 'delayed']))
col4.metric("Cancelled", len(df[df['status'] == 'cancelled']))

st.divider()

# Tabs
tab1, tab2, tab3 = st.tabs(["Overview", "Airlines", "Data"])

with tab1:
    st.write("**Status Distribution**")
    status_counts = df['status'].value_counts()
    fig = px.pie(values=status_counts.values, names=status_counts.index)
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.write("**Airline Performance**")
    airline_stats = df.groupby('airline').size()
    fig = px.bar(airline_stats, title="Flights by Airline")
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.write("**Raw Data**")
    st.dataframe(df, use_container_width=True)

st.caption("Test Dashboard - Sample Data Only")

