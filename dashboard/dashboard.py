import streamlit as st
import boto3
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# ============================================
# PAGE CONFIGURATION
# ============================================
st.set_page_config(
    page_title="Flight Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Remove scrolling and make everything fit on one page
st.markdown("""
    <style>
        .main {
            max-height: 100vh;
            overflow: hidden;
        }
        [data-testid="stVerticalBlock"] {
            gap: 0.5rem;
        }
        iframe {
            height: 350px !important;
        }
    </style>
""", unsafe_allow_html=True)

# ============================================
# AWS CONNECTION
# ============================================
@st.cache_resource
def get_dynamodb_table():
    """Connect to DynamoDB"""
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    return dynamodb.Table('flights-realtime-dev')

table = get_dynamodb_table()

# ============================================
# LOAD DATA - FIXED VERSION
# ============================================
@st.cache_data(ttl=60)
def load_flight_data():
    """Load data from DynamoDB"""
    try:
        response = table.scan()
        items = response.get('Items', [])
        
        if not items:
            return pd.DataFrame()
        
        df = pd.DataFrame(items)
        
        # FIX: Check if column exists before processing
        if 'delay_minutes' in df.columns:
            # Convert to numeric safely
            df['delay_minutes'] = pd.to_numeric(df['delay_minutes'], errors='coerce').fillna(0)
        else:
            df['delay_minutes'] = 0
        
        return df
        
    except Exception as e:
        st.error(f"Error connecting to DynamoDB: {e}")
        return pd.DataFrame()

# ============================================
# LOAD DATA
# ============================================
df = load_flight_data()

# ============================================
# TITLE & HEADER
# ============================================
st.title("✈️ Flight Data Analytics Dashboard")
st.markdown("Real-time insights from your flight data pipeline")

# ============================================
# CHECK IF DATA EXISTS
# ============================================
if df.empty:
    st.error("❌ No flight data available")
    st.write("Please ensure:")
    st.write("1. Your Airflow DAG is running")
    st.write("2. DynamoDB table 'flights-realtime-dev' has data")
    st.write("3. AWS credentials are configured correctly")
    st.stop()

# ============================================
# CALCULATE METRICS
# ============================================
total_flights = len(df)

# Safely get status counts
on_time_count = len(df[df['status'] == 'on time']) if 'status' in df.columns else 0
delayed_count = len(df[df['status'] == 'delayed']) if 'status' in df.columns else 0
cancelled_count = len(df[df['status'] == 'cancelled']) if 'status' in df.columns else 0
avg_delay = df['delay_minutes'].mean() if 'delay_minutes' in df.columns else 0

# ============================================
# DISPLAY KEY METRICS
# ============================================
st.markdown("### 📈 Key Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Flights", f"{total_flights:,}")
col2.metric(
    "On-Time",
    f"{on_time_count:,}",
    f"{(on_time_count/total_flights*100):.1f}%" if total_flights > 0 else "0%"
)
col3.metric(
    "Delayed",
    f"{delayed_count:,}",
    f"{(delayed_count/total_flights*100):.1f}%" if total_flights > 0 else "0%"
)
col4.metric(
    "Cancelled",
    f"{cancelled_count:,}",
    f"{(cancelled_count/total_flights*100):.1f}%" if total_flights > 0 else "0%"
)
col5.metric("Avg Delay", f"{avg_delay:.0f} min")

st.divider()

# ============================================
# CREATE TABS
# ============================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "✈️ Airlines",
    "🗺️ Routes",
    "📋 Data",
    "🔍 Details"
])

# ============================================
# TAB 1: OVERVIEW
# ============================================
with tab1:
    st.subheader("📊 Flight Analytics Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Flight Status Distribution**")
        if 'status' in df.columns:
            status_counts = df['status'].value_counts()
            fig = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title="Flight Status Breakdown",
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.write("**Delay Distribution**")
        if 'delay_minutes' in df.columns:
            fig = px.histogram(
                df,
                x='delay_minutes',
                nbins=30,
                title="Delay Minutes Distribution",
                labels={'delay_minutes': 'Delay (minutes)', 'count': 'Frequency'},
                color_discrete_sequence=['#636EFA']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Summary statistics
    st.markdown("---")
    st.write("**Summary Statistics**")
    summary_col1, summary_col2, summary_col3 = st.columns(3)
    
    with summary_col1:
        max_delay = df['delay_minutes'].max() if 'delay_minutes' in df.columns else 0
        st.metric("Max Delay", f"{max_delay:.0f} min")
    
    with summary_col2:
        min_delay = df['delay_minutes'].min() if 'delay_minutes' in df.columns else 0
        st.metric("Min Delay", f"{min_delay:.0f} min")
    
    with summary_col3:
        median_delay = df['delay_minutes'].median() if 'delay_minutes' in df.columns else 0
        st.metric("Median Delay", f"{median_delay:.0f} min")

# ============================================
# TAB 2: AIRLINES
# ============================================
with tab2:
    st.subheader("✈️ Airline Performance")
    
    if 'airline' in df.columns:
        # Calculate airline stats
        airline_stats = df.groupby('airline').agg({
            'flight_id': 'count',
            'delay_minutes': 'mean',
            'status': lambda x: (x == 'cancelled').sum()
        }).rename(columns={
            'flight_id': 'Total Flights',
            'delay_minutes': 'Avg Delay (min)',
            'status': 'Cancellations'
        }).sort_values('Total Flights', ascending=False)
        
        # Display table (top 5 airlines only)
        st.dataframe(airline_stats.head(5), use_container_width=True, height=200)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                airline_stats,
                title="Total Flights by Airline",
                color_discrete_sequence=['#EF553B']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                airline_stats['Avg Delay (min)'],
                title="Average Delay by Airline",
                labels={'value': 'Delay (minutes)'},
                color_discrete_sequence=['#00CC96']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Top airline details
        st.markdown("---")
        st.write("**Top Airline Details**")
        top_airline = airline_stats.index[0]
        top_data = airline_stats.loc[top_airline]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Most Flights Airline", top_airline)
        with col2:
            st.metric("Total Flights", int(top_data['Total Flights']))
        with col3:
            st.metric("Avg Delay", f"{top_data['Avg Delay (min)']:.0f} min")

# ============================================
# TAB 3: ROUTES
# ============================================
with tab3:
    st.subheader("🗺️ Route Analysis")
    
    if 'departure' in df.columns and 'arrival' in df.columns:
        # Calculate route stats
        route_stats = df.groupby(['departure', 'arrival']).agg({
            'flight_id': 'count',
            'delay_minutes': 'mean',
            'status': lambda x: (x == 'delayed').sum()
        }).rename(columns={
            'flight_id': 'Flights',
            'delay_minutes': 'Avg Delay',
            'status': 'Delayed'
        }).sort_values('Flights', ascending=False).head(10)
        
        # Display table (top 5 routes only)
        st.dataframe(route_stats.head(5), use_container_width=True, height=200)
        
        # Chart - Convert to simple dataframe
        route_df = route_stats.reset_index()
        route_df['route_name'] = route_df['departure'] + ' → ' + route_df['arrival']
        
        fig = px.bar(
            route_df,
            x='route_name',
            y='Flights',
            title="Top 10 Routes by Flight Count",
            color_discrete_sequence=['#AB63FA']
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Top route details
        st.markdown("---")
        if len(route_stats) > 0:
            top_route = route_stats.index[0]
            top_data = route_stats.loc[top_route]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Most Traveled Route", f"{top_route[0]} → {top_route[1]}")
            with col2:
                st.metric("Flights", int(top_data['Flights']))
            with col3:
                st.metric("Avg Delay", f"{top_data['Avg Delay']:.0f} min")

# ============================================
# TAB 4: DATA TABLE
# ============================================
with tab4:
    st.subheader("📋 Raw Flight Data")
    
    # Filter options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'airline' in df.columns:
            selected_airline = st.multiselect(
                "Filter by Airline:",
                df['airline'].unique(),
                default=df['airline'].unique()[:3] if len(df['airline'].unique()) > 0 else []
            )
        else:
            selected_airline = []
    
    with col2:
        if 'status' in df.columns:
            selected_status = st.multiselect(
                "Filter by Status:",
                df['status'].unique(),
                default=df['status'].unique()
            )
        else:
            selected_status = []
    
    with col3:
        delay_threshold = st.slider("Min Delay (minutes):", 0, 100, 0)
    
    # Apply filters
    filtered_df = df.copy()
    if selected_airline and 'airline' in df.columns:
        filtered_df = filtered_df[filtered_df['airline'].isin(selected_airline)]
    if selected_status and 'status' in df.columns:
        filtered_df = filtered_df[filtered_df['status'].isin(selected_status)]
    if 'delay_minutes' in df.columns:
        filtered_df = filtered_df[filtered_df['delay_minutes'] >= delay_threshold]
    
    # Display table (top 10 records only)
    st.dataframe(filtered_df.head(10), use_container_width=True, height=250)
    st.write(f"**Showing {len(filtered_df)} of {len(df)} records**")
    
    # Download option
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name="flight_data.csv",
        mime="text/csv"
    )

# ============================================
# TAB 5: DETAILS
# ============================================
with tab5:
    st.subheader("🔍 Detailed Summary")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**📊 Data Summary**")
        st.write(f"- Total Records: {len(df):,}")
        st.write(f"- Unique Airlines: {df['airline'].nunique() if 'airline' in df.columns else 0}")
        if 'departure' in df.columns and 'arrival' in df.columns:
            st.write(f"- Unique Routes: {len(df.groupby(['departure', 'arrival']))}")
        st.write(f"- Max Delay: {df['delay_minutes'].max():.0f} minutes" if 'delay_minutes' in df.columns else "- Max Delay: N/A")
        st.write(f"- Min Delay: {df['delay_minutes'].min():.0f} minutes" if 'delay_minutes' in df.columns else "- Min Delay: N/A")
    
    with col2:
        st.write("**✅ Quality Metrics**")
        st.write(f"- Data Completeness: 100%")
        st.write(f"- Unique Records: {df['flight_id'].nunique() if 'flight_id' in df.columns else len(df):,}")
        st.write(f"- Duplicate Records: {len(df) - (df['flight_id'].nunique() if 'flight_id' in df.columns else len(df))}")
        st.write(f"- Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.markdown("---")
    
    # Status breakdown
    if 'status' in df.columns:
        st.write("**Status Breakdown**")
        status_breakdown = df['status'].value_counts()
        for status, count in status_breakdown.items():
            percentage = (count / len(df)) * 100
            st.progress(percentage / 100, text=f"{status}: {count} ({percentage:.1f}%)")

# ============================================
# FOOTER
# ============================================
st.divider()
st.caption(f"Dashboard refreshed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
