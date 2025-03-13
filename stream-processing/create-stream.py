import pandas as pd
from tqdm import tqdm
import os
from socket_client import SocketClient
import time
import json

# set the client 
client = SocketClient()
client.connect()

# ============================================
# ==============  stream START ===============
# ============================================

# read and load the data

stocks_files = os.listdir('../data_sourcing/source/real-time')
megadf = pd.DataFrame()

for stock in tqdm(stocks_files[:50]): 
    df = pd.read_csv(f'../data_sourcing/source/real-time/{stock}')
    df = df.drop_duplicates()
    megadf = pd.concat([megadf,df],axis=0)
    
megadf = megadf.dropna()
all_times = megadf['unix_timestamp'].unique()

for a_t in tqdm(all_times):
    stock_df = megadf[megadf['unix_timestamp'] == a_t ]
    stock_dict = {
        "unix_timestamp": stock_df["unix_timestamp"].tolist(),
        "stockname":stock_df['stockname'].tolist(),
        "open": stock_df["open"].tolist(),
        "close": stock_df["close"].tolist(),
        "high": stock_df["high"].tolist(),
        "low": stock_df["low"].tolist(),
        "volume": stock_df["volume"].tolist()
    }
    stock_data = json.dumps(stock_dict)
    client.send_message(stock_data)
    time.sleep(1)
    
# ============================================
# ===============  stream END ================
# ============================================