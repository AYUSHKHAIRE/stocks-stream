from spark_stream_manager import SparkStreamManager
import streamlit as st
import threading
import time

# Initialize spark manager
SSM = SparkStreamManager()
SSM.start_stream_session()
SSM.set_spark_schema()
SSM.start_raw_stream_from_socket()

# Start the stream query in a separate thread (only if not already running)
def start_stream_query():
    try:
        # Only start if not already running
        active_queries = [q.name for q in SSM.spark.streams.active]
        if "stock_meta_table" not in active_queries:
            SSM.define_stock_aggregation_query()
        else:
            print("[INFO] Stream query already running.")
    except Exception as e:
        print("[ERROR] Failed to start stream query:", e)

threading.Thread(target=start_stream_query, daemon=True).start()

# UI layout
st.header("Real Time Stocks Data LIVE")
placeholder = st.empty()

# Streamlit-friendly loop (avoiding infinite loop for Streamlit)
while True:
    try:
        stock_meta_df = SSM.read_stream_table("stock_meta_table")
        placeholder.dataframe(stock_meta_df)
        time.sleep(1)  # Update every second
    except Exception as e:
        placeholder.warning("Waiting for stream data to load...")
        time.sleep(2)
