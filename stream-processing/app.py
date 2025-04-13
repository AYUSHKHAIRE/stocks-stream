from spark_stream_manager import SparkStreamManager
import streamlit as st
import threading
import time
from logger_config import logger

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

def start_the_window(window_details={
    'window_size': '1 minute',
    'window_slide': '1 second',
    'window_watermark': '5 minutes',
    'window_processing_time': '1 second'
}):
    SSM.set_window_query_with_limits(
        window_size=window_details['window_size'],
        window_slide=window_details['window_slide'],
        window_watermark=window_details['window_watermark'],
        window_processing_time=window_details['window_processing_time']
    )

threading.Thread(target=start_stream_query, daemon=True).start()
threading.Thread(target=start_the_window, daemon=True).start()

# UI layout
st.header("Real Time Stocks Data LIVE")
meta_query_placeholder = st.empty()
st.header("Window Aggregated Data")
st.subheader("Window Aggregated Data ( past 1 minute , updating in one second )")
window_query_placeholder = st.empty()


# Streamlit-friendly loop (avoiding infinite loop for Streamlit)
while True:
    try:
        stock_meta_df = SSM.read_stream_table("stock_meta_table")
        meta_query_placeholder.dataframe(stock_meta_df)
        time.sleep(1)  # Update every second
        window_df = SSM.read_stream_table("window_query_1_minute_1_second_5_minutes_1_second")
        window_query_placeholder.dataframe(window_df)
        time.sleep(1)  # Update every second
    except Exception as e:
        window_query_placeholder.warning("Waiting for stream data to load...")
        meta_query_placeholder.warning("Waiting for stream data to load...")
        time.sleep(1)
        logger.error(f"Error in Streamlit loop: {e}")
