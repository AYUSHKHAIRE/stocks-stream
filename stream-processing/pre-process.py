import pandas as pd
import os 
from tqdm import tqdm
from datetime import datetime

stocks_files = os.listdir('../data_sourcing/source/real-time')

for stock in tqdm(stocks_files):
    df = pd.read_csv(f'../data_sourcing/source/real-time/{stock}')
    
    df['unix_timestamp'] = df['timestamp'].apply(
        lambda x: int(
            datetime.strptime(x, '%Y-%m-%d %H:%M:%S').timestamp()
        )
    )

    df = df.sort_values(by='unix_timestamp')

    df.to_csv(f'../data_sourcing/source/real-time/{stock}', index=False)
