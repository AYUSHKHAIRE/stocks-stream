import pandas as pd
from tqdm import tqdm
import os
from socket_client import SocketClient
import time
import json

# Initialize socket client
client = SocketClient()
client.connect()

# Load stock data
stocks_dir = '../data_sourcing/source/real-time'
stocks_files = os.listdir(stocks_dir)
megadf = pd.DataFrame()

for stock in tqdm(stocks_files[:10]): 
    df = pd.read_csv(os.path.join(stocks_dir, stock))
    df = df.drop_duplicates()
    megadf = pd.concat([megadf, df], axis=0)

megadf.dropna(inplace=True)
all_times = megadf['unix_timestamp'].unique()

# Stream stock data row-by-row
for a_t in tqdm(all_times):
    stock_df = megadf[megadf['unix_timestamp'] == a_t]
    
    for _, row in stock_df.iterrows():  
        stock_dict = {
            "unix_timestamp": int(row["unix_timestamp"]),
            "stockname": row["stockname"],
            "open": float(row["open"]),
            "close": float(row["close"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "volume": float(row["volume"])
        }
        stock_data = json.dumps(stock_dict)
        client.send_message(stock_data)
        time.sleep(0.1)  # Reduce sleep for better performance

client.close()
