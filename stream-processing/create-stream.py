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
    megadf = pd.concat([megadf,df],axis=0)
    
all_times = megadf['unix_timestamp'].unique()

for a_t in tqdm(all_times):
    stock_df = megadf[megadf['unix_timestamp'] == a_t ]
    stock_dict = stock_df.to_dict()
    stock_data = json.dumps(stock_dict)
    client.send_message(stock_data)
    time.sleep(1)
    
# ============================================
# ===============  stream END ================
# ============================================