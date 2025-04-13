from spark_stream_manager import SparkStreamManager
import streamlit as st
import threading
import time
from logger_config import logger
import seaborn as sns
from matplotlib import pyplot as plt

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

window_details_to_analyze = [
    {
        'window_size': '15 seconds',
        'window_slide': '1 second',
        'window_watermark': '1 minutes',
        'window_processing_time': '1 second'
    },
    {
        'window_size': '30 seconds',
        'window_slide': '1 second',
        'window_watermark': '1 minutes',
        'window_processing_time': '1 second'
    },
    {
        'window_size': '1 minute',
        'window_slide': '1 second',
        'window_watermark': '5 minutes',
        'window_processing_time': '1 second'
    },
    {
        'window_size': '5 minutes',
        'window_slide': '1 second',
        'window_watermark': '10 minutes',
        'window_processing_time': '1 second'
    }
]

def start_the_window(window_details):
    SSM.set_window_query_with_limits(
        window_size=window_details['window_size'],
        window_slide=window_details['window_slide'],
        window_watermark=window_details['window_watermark'],
        window_processing_time=window_details['window_processing_time']
    )

# Start the window queries in separate threads
def start_window_queues():
    for wd in window_details_to_analyze:
        threading.Thread(target=start_the_window, args=(wd,) , daemon=True).start()
        time.sleep(1)  
        
threading.Thread(target=start_stream_query, daemon=True).start()
threading.Thread(target=start_window_queues , daemon=True).start()

# Set global dark theme
sns.set_theme(style="darkgrid", palette="muted")  # Or try 'deep', 'bright', etc.
plt.style.use("dark_background")  # For dark plot background

# UI layout
st.header("Real Time Stocks Data LIVE")
meta_query_placeholder = st.empty()
st.header("Window Aggregated Data")

window_query_placeholder_pool = {}
window_chart_placeholder_pool = {}
window_time_wise_chart_placeholder_pool = {}

for wd in window_details_to_analyze:
    st.write(f"Window Aggregated Data (past {wd['window_size']}, updating every {wd['window_slide']})")
    queryname = f"window_query_{wd['window_size']}_{wd['window_slide']}_{wd['window_watermark']}_{wd['window_processing_time']}".replace(" ", "_")
    col1, col2 = st.columns([1, 1.5])  
    window_query_placeholder_pool[queryname] = col1.empty()
    with col2:
        with st.spinner("Generating plot..."):
            window_chart_placeholder_pool[queryname] = st.empty()
    window_time_wise_chart_placeholder_pool[queryname] = st.empty()

# Streamlit-friendly loop (avoiding infinite loop for Streamlit)
while True:
    try:
        stock_meta_df = SSM.read_stream_table("stock_meta_table")
        meta_query_placeholder.dataframe(stock_meta_df)
        time.sleep(1)

        for wd in window_details_to_analyze:
            queryname = f"window_query_{wd['window_size']}_{wd['window_slide']}_{wd['window_watermark']}_{wd['window_processing_time']}".replace(" ", "_")

            # Initialize placeholders only once
            if queryname not in window_query_placeholder_pool:
                window_query_placeholder_pool[queryname] = st.empty()
            if queryname not in window_chart_placeholder_pool:
                window_chart_placeholder_pool[queryname] = st.empty()
            if queryname not in window_time_wise_chart_placeholder_pool:
                window_time_wise_chart_placeholder_pool[queryname] = st.empty()

            window_df = SSM.read_stream_table(queryname)
            window_query_placeholder_pool[queryname].dataframe(window_df)

            if not window_df.empty and {'max_unix_timestamp', 'price_diff', 'stockname'}.issubset(window_df.columns):
                try:
                    plot_df = window_df.head(100)
                    # Create a seaborn line plot with matplotlib
                    fig, ax = plt.subplots(figsize=(8, 8))
                    sns.barplot(
                        data=plot_df,
                        x='stockname',
                        y='price_diff',
                        hue='stockname',  # Ensures different colors
                        dodge=False, 
                        ax=ax
                    )
                    ax.set_title("Price Difference over Time")
                    ax.set_xlabel("Timestamp")
                    ax.set_ylabel("Price Difference")
                    ax.tick_params(axis='x', rotation=45)
                    ax.grid(True)
                    window_chart_placeholder_pool[queryname].pyplot(fig)
                    # Get top 10 stocks based on max price_diff
                    top_10_stocks = window_df.groupby('stockname')['price_diff'].max().nlargest(10).index.tolist()
                    time_df = window_df[window_df['stockname'].isin(top_10_stocks)].sort_values(by='max_unix_timestamp')
                    fig, ax = plt.subplots(figsize=(14, 6))
                    for stock in top_10_stocks:
                        stock_df = time_df[time_df['stockname'] == stock]
                        ax.plot(stock_df['max_unix_timestamp'], stock_df['price_diff'], label=stock, marker='o')
                    ax.set_title("Top 10 Stocks - Price Difference Over Time", fontsize=16, color='white')
                    ax.set_xlabel("Timestamp", color='white')
                    ax.set_ylabel("Price Difference", color='white')
                    ax.tick_params(axis='x', rotation=45, colors='white')
                    ax.tick_params(axis='y', colors='white')
                    ax.grid(True)
                    ax.set_facecolor("#222222")
                    ax.legend(title="Stock", loc='upper left', fontsize='small')
                    fig.tight_layout()
                    window_time_wise_chart_placeholder_pool[queryname].pyplot(fig)
                    plt.close(fig)  # Close the figure after rendering
                except Exception as e:
                    window_chart_placeholder_pool[queryname].warning(f"Failed to plot: {e}")
            else:
                window_chart_placeholder_pool[queryname].info("Waiting for valid data...")

    except Exception as e:
        for wd in window_details_to_analyze:
            queryname = f"window_query_{wd['window_size']}_{wd['window_slide']}_{wd['window_watermark']}_{wd['window_processing_time']}".replace(" ", "_")
            window_query_placeholder_pool[queryname].info("Loading the data ...")
            window_chart_placeholder_pool[queryname].info("Loading the graph ...")
        meta_query_placeholder.info("Loading the data ...")
        time.sleep(1)
        logger.error(f"Error in Streamlit loop: {e}")