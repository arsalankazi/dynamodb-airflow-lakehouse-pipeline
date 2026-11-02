import os
import json
import pandas as pd
import plotly.express as px
import streamlit as st
import boto3

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
DASHBOARD = os.getenv("DASHBOARD_BUCKET", "ecommerce-dashboard")

s3 = boto3.client(
    "s3", 
    endpoint_url=ENDPOINT, 
    region_name="us-east-1", 
    aws_access_key_id="test", 
    aws_secret_access_key="test"
)

st.set_page_config(page_title="E-commerce Lakehouse", layout="wide")
st.title("E-commerce Orders Lakehouse Dashboard")
st.markdown("Real-time insights from DynamoDB Streams & PySpark ETL")

try:
    # Fetch the JSON data from S3
    response = s3.get_object(Bucket=DASHBOARD, Key="analytics/summary.json")
    data = json.loads(response['Body'].read().decode('utf-8'))
    df = pd.DataFrame(data)

    # Top KPI Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Orders", int(df['order_count'].sum()))
    col2.metric("Total Revenue", f"${df['total_revenue'].sum():,.2f}")
    col3.metric("Statuses Tracked", len(df))

    st.divider()

    # Charts
    col4, col5 = st.columns(2)
    
    with col4:
        st.subheader("Revenue by Status")
        fig_bar = px.bar(df, x='status', y='total_revenue', color='status', text_auto='.2s')
        st.plotly_chart(fig_bar, use_container_width=True)

    with col5:
        st.subheader("Order Volume by Status")
        fig_pie = px.pie(df, names='status', values='order_count', hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)

except Exception as e:
    st.error(f"Dashboard data not ready yet. Please run the analytics query first. Error: {e}")