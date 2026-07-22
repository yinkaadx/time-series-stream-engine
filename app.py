import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import time

st.set_page_config(page_title="Distributed Stream Processing", layout="wide")

st.title("Serverless Data-in-Motion Filtering Pipeline")
st.caption("Distributed Stream Processing Engine for High-Velocity Time-Series Databases (InfluxDB/TimescaleDB Simulation)")

st.sidebar.header("Stream Configuration")
selected_network = st.sidebar.selectbox("Target Telemetry Network", ["National Grid Substation Alpha", "Enterprise Smart Office Array", "Industrial HVAC Sensor Net"])
stream_velocity = st.sidebar.slider("Simulate Raw Data Velocity (Nodes/sec)", 5000, 50000, 20000)
run_simulation = st.sidebar.button("Initialize Distributed Stream Engine")

st.sidebar.markdown("---")
st.sidebar.caption("Architecture: AWS Kinesis -> Lambda In-Transit Filter -> Time-Series DB")

if run_simulation:
    st.subheader(f"Active Telemetry Stream: {selected_network}")
    
    col1, col2, col3, col4 = st.columns(4)
    metric_raw = col1.empty()
    metric_filtered = col2.empty()
    metric_latency = col3.empty()
    metric_status = col4.empty()

    chart_placeholder = st.empty()
    log_placeholder = st.empty()

    np.random.seed(303)
    time_steps = pd.date_range(start=pd.Timestamp.now(), periods=100, freq="s")
    
    raw_payload_size = []
    filtered_payload_size = []
    
    total_raw_nodes = 0
    total_filtered_nodes = 0
    
    for i in range(100):
        current_raw = int(stream_velocity + np.random.uniform(-1000, 2500))
        compression_ratio = np.random.uniform(0.35, 0.45) 
        current_filtered = int(current_raw * compression_ratio)
        
        raw_payload_size.append(current_raw)
        filtered_payload_size.append(current_filtered)
        
        total_raw_nodes += current_raw
        total_filtered_nodes += current_filtered
        
        metric_raw.metric("Raw Data Velocity (Nodes/s)", f"{current_raw:,}")
        metric_filtered.metric("Filtered DB Writes (Nodes/s)", f"{current_filtered:,}", f"-{int((1-compression_ratio)*100)}% load")
        metric_latency.metric("Stream Ingestion Latency", f"{np.random.uniform(12.5, 18.2):.1f} ms")
        metric_status.metric("TSDB Health", "OPTIMIZED", "Stable")
            
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=time_steps[:i+1], y=raw_payload_size, mode='lines', name='Raw Data Load (Unfiltered)', line=dict(color='red')))
        fig.add_trace(go.Scatter(x=time_steps[:i+1], y=filtered_payload_size, mode='lines', name='Data-in-Motion Writes (Filtered)', fill='tozeroy', line=dict(color='green')))
        
        fig.update_layout(
            title="Real-Time Data-in-Motion Ingestion vs Time-Series Database Write Load",
            xaxis=dict(title="Stream Timestamp"),
            yaxis=dict(title="Data Nodes Processed"),
            height=400,
            margin=dict(l=0, r=0, t=40, b=0)
        )
        
        chart_placeholder.plotly_chart(fig, use_container_width=True)
        
        if i % 5 == 0:
            log_placeholder.warning(f"STREAM LOG: High-velocity spike intercepted at {time_steps[i].strftime('%H:%M:%S')}. Lambda middleware aggregated redundant telemetry. Database persistence load reduced.")
        else:
            log_placeholder.success(f"STREAM LOG: Micro-batch {i} processed in motion. Clean time-series vectors successfully written to persistent storage.")
            
        time.sleep(0.15)
        
    st.info(f"Simulation Complete. Total Raw Nodes: {total_raw_nodes:,}. The distributed middleware successfully prevented {total_raw_nodes - total_filtered_nodes:,} redundant records from impacting the Time-Series Database.")
else:
    st.info("Click 'Initialize Distributed Stream Engine' in the sidebar to simulate high-frequency data-in-motion processing.")