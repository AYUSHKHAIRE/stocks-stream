import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import time 
from tqdm import tqdm
import os
from datetime import datetime,timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging
from kaggle_secrets import UserSecretsClient
import warnings
import json
import shutil
import subprocess
import gc

warnings.filterwarnings('ignore')

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


logger.info("This is an INFO message")

user_secrets = UserSecretsClient()
kaggle_apikey = user_secrets.get_secret("kaggle_apikey")
kaggle_username = user_secrets.get_secret("kaggle_username")
mngodb_database_name = user_secrets.get_secret("mngodb_database_name")
mongodb_app_name = user_secrets.get_secret("mongodb_appname")
mongodb_password = user_secrets.get_secret("mongodb_password")
mongodb_username = user_secrets.get_secret("mongodb_username")
mongodb_cluster_name = user_secrets.get_secret("mongodb_cluster_name")

class AtlasClient:
    def __init__(self, atlas_uri, dbname):
        self.mongodb_client = MongoClient(atlas_uri)
        self.database = self.mongodb_client[dbname]

    def ping(self):
        try:
            self.mongodb_client.admin.command('ping')
            logging.info("Pinged your MongoDB deployment. Connection successful.")
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")

    def get_collection(self, collection_name):
        collection = self.database[collection_name]
        return collection

    def findOneByKey(self,collection_name,key):
        collection = self.get_collection(collection_name)
        result = collection.find_one({ key: { "$exists": True } })
        return result


    def find(self, collection_name, filter={}, limit=0):
        collection = self.database[collection_name]
        items = list(collection.find(filter=filter, limit=limit))
        return items
    
    def insert(self, collection_name, documents):
        """
        Inserts one or more documents into a MongoDB collection.
        
        Parameters:
        - collection_name: str, the name of the collection
        - documents: dict or list of dicts, the document(s) to insert
        
        If `documents` is a list, it will insert multiple documents using `insert_many`.
        Otherwise, it will insert a single document using `insert_one`.
        """
        collection = self.get_collection(collection_name)
        
        if isinstance(documents, list):
            result = collection.insert_many(documents)
            return result.inserted_ids
        else:
            result = collection.insert_one(documents)
            return result.inserted_id
        
    def delete(self, collection_name, filter={}, _del_all_=False):
        """
        Deletes documents from a MongoDB collection based on the filter.
        
        Parameters:
        - collection_name: str, the name of the collection.
        - filter: dict, the filter to find documents to delete (default is {}).
        - _del_all_: bool, if True, deletes all documents matching the filter using `delete_many()`.
                      If False, deletes only one document using `delete_one()`.
        
        Returns:
        - Number of documents deleted.
        """
        collection = self.get_collection(collection_name)
        
        if _del_all_:
            result = collection.delete_many(filter)
            return result.deleted_count
        else:
            result = collection.delete_one(filter)
            if result.deleted_count == 1:
                pass
            else:
                pass
            return result.deleted_count
        
AC = AtlasClient(
    atlas_uri=f"mongodb+srv://{mongodb_username}:{mongodb_password}@{mongodb_cluster_name}.fznbh.mongodb.net/?retryWrites=true&w=majority&appName={mongodb_app_name}",
    dbname = mngodb_database_name
)


class stocksManager:
    def __init__(self) -> None:
        self.available_stocks = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        self.headers = headers
        self.firstrun = 0

    def collect_stock_symbols(self):
        targets = [
            'most-active',
            'gainers',
            'losers',
        ]   
    
        limitlist = []

        for page in tqdm(targets):
            url = f'https://finance.yahoo.com/{page}/?offset=0&count=100'
            try:
                r = rq.get(url,headers = self.headers)
            except Exception as e:
                logger.warning("cannot hit url : ",url ,e,r.status_code)
            soup = BeautifulSoup(r.text,'html.parser')
            limits = soup.find(
                'div',{'class':'total yf-1tdhqb1'}
            ).text
            limits = limits.split(' ')[2]
            limitlist.append(limits)

        max_hits = []
        for limit in limitlist:
            max_hit = int(int(limit) / 100)
            max_hits.append(max_hit)

        findict = {
            'targets':targets,
            'max_hits':max_hits
        }
        
        urls_for_stocks = []

        i = 0
        for i in range(
            len(
                findict['targets']
                )
            ):
            target = findict['targets'][i]
            maxhit = findict['max_hits'][i]
            for m in range(maxhit+1):
                url = f'https://finance.yahoo.com/markets/stocks/{target}/?start={m*100}&count=100/'
                urls_for_stocks.append(url)

        data = []

        logger.info('collecting data for symbols _______________________________--')
        for u in urls_for_stocks:
            catg = u.split('/')[-3]
            symbol_list = []
            try:
                r = rq.get(u,headers = self.headers)
            except Exception as e:
                logger.warning("cannot hit url : ",u ,r.status_code)
            soup = BeautifulSoup(r.text,'html.parser')
            symbs= soup.find_all('span',{'class':'symbol'})
            for s in symbs:
                symbol_list.append(s.text)
            data.append(
                {catg:symbol_list}
            )
        logger.info("finished collecting data for symbols ______________________________-")
        data = {'names':data}
        return data
    
    def return_list_for_symbols(self):
        symbols = self.collect_stock_symbols()
        finals_symbols = []
        for n in symbols['names']:
            for key in n.keys():
                finals_symbols=finals_symbols+n[key]
        finals_symbols = list(set(finals_symbols))
        return finals_symbols

    def return_human_timestamp(self, timestamps):
            if isinstance(timestamps, list):
                new_dates = []
                for unix_time in timestamps:
                    try:
                        if isinstance(unix_time, str):
                            datetime.strptime(unix_time, '%Y-%m-%d %H:%M:%S') 
                            new_dates.append(unix_time)
                        else:
                            unix_time = float(unix_time)
                            date = datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
                            new_dates.append(date)
                    except (ValueError, TypeError):
                        new_dates.append(None)  
                return new_dates
            elif isinstance(timestamps, str):
                try:
                    unix_time = float(timestamps)
                    date = datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
                    return date
                except (ValueError, TypeError):
                    return None

    def return_unix_timestamps(self, date_strings):
        if isinstance(date_strings, list):
            unix_timestamps = []
            for date_str in date_strings:
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    unix_timestamp = int(dt.timestamp())  
                    unix_timestamps.append(unix_timestamp)
                except (ValueError, TypeError):
                    unix_timestamps.append(None)
            return unix_timestamps
        elif isinstance(date_strings, str):
            try:
                dt = datetime.strptime(date_strings, '%Y-%m-%d %H:%M:%S')
                unix_timestamp = int(dt.timestamp())  
                return unix_timestamp
            except (ValueError, TypeError):
                return None

    def update_prices_for_daily(self, symbol_list):
        current_timestamp = int(time.time())
        current_time = datetime.fromtimestamp(current_timestamp)
        human_readable_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Start and end periods for data retrieval
        start_date_str = "2015-01-01"
        start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
        period1 = int(time.mktime(start_date_obj.timetuple()))
        period2 = current_timestamp
        
        logger.warning(f"Daily data for today's date {human_readable_time}")
        logger.info(f"Checking updates for period1={period1} & period2={period2} for stocks daily")

        # Define base path for daily updates
        files_path = f'/kaggle/working/daily_update/'
        os.makedirs('/kaggle/working/daily_update/',exist_ok = True)
        os.makedirs('/kaggle/working/daily_update_to_kaggle/',exist_ok = True)
        AC.delete(
            "daily_data",
            _del_all_ = True
        )
        for stock in tqdm(symbol_list):
            stock_symbol = stock.replace(' ', '')
            json_path = f'{files_path}/{stock_symbol}.json'
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            
            url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{stock_symbol}?events=capitalGain%7Cdiv%7Csplit'
                f'&formatted=true&includeAdjustedClose=true&interval=1d&period1={period1}&period2={period2}'
                f'&symbol={stock_symbol}&userYfid=true&lang=en-US&region=US')
            try:
                response = rq.get(url, headers=self.headers)
                if response.status_code == 200:
                    with open(json_path, 'wb') as file:
                        file.write(response.content)
                    json_data = pd.read_json(json_path)
                    timestamp = json_data['chart']['result'][0].get('timestamp')
                    if timestamp:
                        new_timestamps = self.return_human_timestamp(timestamp)
                        new_data = json_data['chart']['result'][0]['indicators']['quote'][0]
                        new_data['timestamp'] = new_timestamps
                        data_to_insert = {f'{stock_symbol}':new_data}
                        if data_to_insert:
#                             in database
                            AC.insert(
                                collection_name="daily_data",
                                documents=data_to_insert
                            )
#                            local
                            new_data = pd.DataFrame(new_data)
                            new_data.to_csv(f'/kaggle/working/daily_update_to_kaggle/{stock}.csv')
                            
                        else:
                            logger.error(f'daily data insertion for {stock} failed .',e)
                else:
                    logger.warning(f"Request failed: {url}, Status code: {response.status_code}")
                    continue
            except:
                continue
        logger.info("Daily data update finished.")
  
    
    def update_prices_for_per_minute(self, symbol_list,last_date):
        os.makedirs(f'/kaggle/working/per_minute/', exist_ok=True)
        os.makedirs(f'/kaggle/working/per_minute_to_kaggle/', exist_ok=True) 
        date_time_obj = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
        period1 = int(date_time_obj.timestamp())
        seven_days_back = date_time_obj - timedelta(days=7)
        period2 = int(seven_days_back.timestamp())
  
        logger.info(f"Checking updates for period1={period1} & period2={period2} for stocks per minute.")
            
        AC.delete(
                collection_name="per_minute_data",
                _del_all_ = True
        )
        if symbol_list:
            for stock in tqdm(symbol_list):
                try:
                    stock_symbol = stock.replace(' ', '')
                    link = f'https://query2.finance.yahoo.com/v8/finance/chart/{stock_symbol}?period1={period2}&period2={period1}&interval=1m&includePrePost=true&events=div%7Csplit%7Cearn&&lang=en-US&region=US'
                    response = rq.get(link, headers=self.headers)
                    tmppath = f'/kaggle/working/per_minute/{stock_symbol}.json'
                    if response.status_code == 200:
                        with open(tmppath, 'wb') as jsn:
                            jsn.write(response.content)
                        json_data  = pd.read_json(tmppath)
                        timestamp = json_data['chart'][0][0]['timestamp']
                        json_data = json_data['chart'][0][0]["indicators"]["quote"][0]
                        try:
                            new_timestamps = self.return_human_timestamp(timestamp)
                            json_data['timestamp'] = new_timestamps
                            data_to_insert = {f'{stock_symbol}':json_data}
    #                         to database
                            if data_to_insert:
                                AC.insert(
                                    collection_name="per_minute_data",
                                    documents=data_to_insert
                                )
    #                           to csv
                                json_data = pd.DataFrame(json_data)
                                json_data.to_csv(f'/kaggle/working/per_minute_to_kaggle/{stock}.csv')
                            else:
                                logger.warning(f'per minute data insertion data insertion for {stock} failed .'),e

                        except Exception as e:
                            logger.warning(f"Request failed: {link}, Status code: {response.status_code}")
                            print('failed',e)
                            continue
                except:
                    continue
                
        else:
            logger.warning("It is not Sunday today. Skipping the update step.")
        logger.info("Per minute update finished.")

    def update_stocks_list_for_today(self):
        AC.delete('master',_del_all_ = True)
        stocks = AC.find("daily_data")
        stockslist = []
        for st in tqdm(stocks):
            stockslist.append(list(st.keys())[1])
        self.available_stocks = stockslist
        AC.insert("master",{'stocks':stockslist})
        logger.warning("stocks list updated !")
        
    # specific to kaggle
    def Kaggle_process_daily_data(self,symbol_list):
        self.update_prices_for_daily(symbol_list)
        megadatadailyframe = pd.DataFrame()
        daily_files_csv = os.listdir('/kaggle/working/daily_update_to_kaggle')

        for csv in tqdm(daily_files_csv):
            df = pd.read_csv(f'/kaggle/working/daily_update_to_kaggle/{csv}')
            df['stockname'] = csv.split('.')[0]
            megadatadailyframe = pd.concat([megadatadailyframe,df],axis = 0)
            
        os.makedirs(f'/kaggle/working/daily_update_to_kaggle_final',exist_ok = True)
        megadatadailyframe.to_csv('/kaggle/working/daily_update_to_kaggle_final/stocks.csv')
    
    def Kaggle_process_per_minute_data(self,symbol_list,last_date,weekday):
        self.update_prices_for_per_minute(symbol_list,last_date)
        megadataperminuteframe = pd.DataFrame()
        perminute_files_csv = os.listdir('/kaggle/working/per_minute_to_kaggle/')

        for csv in tqdm(perminute_files_csv):
            df = pd.read_csv(f'/kaggle/working/per_minute_to_kaggle/{csv}')
            df['stockname'] = csv.split('.')[0]
            megadataperminuteframe = pd.concat([megadataperminuteframe,df],axis = 0)
        os.makedirs(f'/kaggle/working/per_minute_to_kaggle_final',exist_ok = True)
        past_df = pd.read_csv("/kaggle/input/real-time-stocks-data/stocks.csv")
        megadataperminuteframe = pd.concat([megadataperminuteframe,past_df],axis = 0)
        megadataperminuteframe = megadataperminuteframe.drop_duplicates()
        megadataperminuteframe = megadataperminuteframe[['low','high','volume','open','close','stockname','timestamp']]
        megadataperminuteframe.to_csv('/kaggle/working/per_minute_to_kaggle_final/stocks.csv')
        
class recoverData:
    def __init__(self):
        self.symbol_list = None
        self.avdata = None
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self.batches_to_check = []
        self.recoverlist = []

    def setup(self):
        os.makedirs("/kaggle/working/per_minute_recover/json/", exist_ok=True)
        os.makedirs("/kaggle/working/per_minute_recover/csv/", exist_ok=True)
        os.makedirs("/kaggle/working/per_minute_recover/final/", exist_ok=True)
        print("set up directories")
        self.avdata = pd.read_csv('/kaggle/input/real-time-stocks-data/stocks.csv')
        self.symbol_list = self.avdata['stockname'].unique()
        print("stock list and dataset loaded .")

    def return_unix_timestamps(self, date_strings):
        try:
            dt = datetime.strptime(date_strings, '%Y-%m-%d')
            return int(dt.timestamp())
        except ValueError:
            return None

    def return_human_timestamp(self, timestamps):
        try:
            return [datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps]
        except Exception:
            return None

    def collect_per_minute_data(self, stock_symbol, start_date, end_date):
        period1 = start_date
        period2 = end_date
        if period1 is None or period2 is None:
            print(f"Invalid dates provided: {start_date}, {end_date}")
            return

        stock_symbol = stock_symbol.replace(' ', '')
        link = f"https://query2.finance.yahoo.com/v8/finance/chart/{stock_symbol}?period1={period1}&period2={period2}&interval=1m&includePrePost=true&events=div%7Csplit%7Cearnings&lang=en-US&region=US"
        response = rq.get(link, headers=self.headers)

        if response.status_code == 200:
            tmppath = f'/kaggle/working/per_minute_recover/json/{stock_symbol}.json'
            with open(tmppath, 'wb') as jsn:
                jsn.write(response.content)
            json_data = response.json()

            try:
                timestamps = json_data['chart']['result'][0]['timestamp']
                indicators = json_data['chart']['result'][0]['indicators']['quote'][0]
                indicators['timestamp'] = self.return_human_timestamp(timestamps)

                df = pd.DataFrame(indicators)
                os.makedirs(f'/kaggle/working/per_minute_recover/csv/{stock_symbol}/',exist_ok = True)
                df.to_csv(
                    f"/kaggle/working/per_minute_recover/csv/{stock_symbol}/{start_date}_{end_date}.csv",
                    index=False,
                )
            except KeyError as e:
                print(f"Error processing JSON data: {e}")
        else:
            print(f"Failed to fetch data for {stock_symbol}. Status code: {response.status_code}")

    def setup_batches(self):
        today = datetime.now()
        while today.weekday() != 6:  # Find the latest sunday
            today -= timedelta(days=1)
        
        # Generate the four weeks from today (Monday to Friday)
        self.batches_to_check = []
        for i in range(4):  # Generate 4 weeks
            end_date = today - timedelta(weeks=i)
            start_date = end_date - timedelta(days=6)  # Monday
            self.batches_to_check.append((
                start_date.replace(hour=0, minute=0, second=0), 
                end_date.replace(hour=23, minute=59, second=59)
            ))
        print(f"Generated batches to check")
    
    def chheck_stock_for_batch(self, symbol):
        newdf = self.avdata[self.avdata['stockname'] == symbol]
        newdf['dates'] = pd.to_datetime(newdf['timestamp']).dt.date
        available_dates = set(newdf['dates'])    
        missing_weeks = []

        for start_date, end_date in self.batches_to_check:
            # Business days: Monday to Friday
            week_dates = set(pd.date_range(start=start_date, end=end_date, freq='B').date)
            
            # Check if all the business days (Monday to Friday) are missing
            if week_dates.isdisjoint(available_dates):
                # print(f"{symbol} Week {start_date} to {end_date} is entirely missing (all weekdays missing)")
                # Convert start_date and end_date to Unix timestamps
                start_unix = int(start_date.timestamp())
                end_unix = int(end_date.timestamp())
                missing_weeks.append([symbol, start_unix, end_unix])
        return missing_weeks

    def detect_all_stocks(self):
        all_stocks = self.avdata['stockname'].unique()
        self.setup_batches()  
        for st in tqdm(all_stocks):
            missing_weeks = self.chheck_stock_for_batch(st)
            self.recoverlist = self.recoverlist + missing_weeks
        print("setup for targets completed")
        return self.recoverlist

    def download_bunches_mass(self,targets):
        print("starting scrappers")
        for item in tqdm(targets):
            self.collect_per_minute_data(item[0],item[1],item[2])

    def merge_new_data(self):
        newframe = pd.DataFrame()
        print("starting mergers")
        all_csvs = os.listdir('/kaggle/working/per_minute_recover/csv/')
        for a_csv in tqdm(all_csvs):
            all_files = os.listdir(f'/kaggle/working/per_minute_recover/csv/{a_csv}')
            for a_file in all_files:
                tmpdf = pd.read_csv(f'/kaggle/working/per_minute_recover/csv/{a_csv}/{a_file}')
                tmpdf['stockname'] = a_csv.split('.')[0]
                newframe = pd.concat([newframe,tmpdf],axis = 0)
        print("finishing mergers")
        return newframe

    def final_merge(self,newcollecteddf):
        newdf = pd.concat([self.avdata,newcollecteddf])
        del self.avdata
        gc.collect()
        print("prepared new dataframe")
        newdf = newdf[['open','high','low','close','stockname','timestamp']]
        newdf.to_csv('/kaggle/working/per_minute_recover/final/stocks.csv')
        print("wrote new file")
        print("finishing main merge")

    def create_metadata_to_push_recover(self):
        print('Creating metadata file for per minute data>>>>')
        data = {
            "id": "ayushkhaire/real-time-stocks-data"
        }
        metadata_file_location = '/kaggle/working/per_minute_recover/final/dataset-metadata.json' 
        with open(metadata_file_location, 'w', encoding='utf-8') as metadata_file:
            json.dump(data, metadata_file)
        print('Metadata file created for per minute data')

    def upload_recovered_to_kaggle(self):
        os.environ['KAGGLE_USERNAME'] = kaggle_username
        os.environ['KAGGLE_KEY'] = kaggle_apikey
        retries = 0
        while retries < 5:
            try:
                command = "kaggle datasets version -p '/kaggle/working/per_minute_recover/final' -m 'Update' -r zip"
                subprocess.run(command, shell=True, check=True)
                logger.info("Upload completefor per minute data")
                break
            except Exception as error:
                logger.error(f"Error from Kaggle: {error}")
                time.sleep(5)
                retries += 1  
# ndf = RCC.avdata[RCC.avdata['stockname'] == "PLTR"]
# ndf['d'] = ndf['timestamp'].str.split(" ").str[0]  # Extract only the date part
# print(ndf['d'].unique())  # Print unique dates for manual verification
# ndf

#  make force - True when notebook fails and do not update per minute data , and give saturdday data
force = False
missinng_saturday_date = "2024-12-28 23:59:59"
AC.delete("daily_data",_del_all_ = True)
# AC.delete("per_minute_data",_del_all_ = True)

STM = stocksManager()
symbols = STM.collect_stock_symbols()
finals_symbols = []
for n in symbols['names']:
    for key in n.keys():
        finals_symbols=finals_symbols+n[key]
finals_symbols = list(set(finals_symbols))

STM.Kaggle_process_daily_data(finals_symbols)
today = datetime.now()
if force == True:
    if missinng_saturday_date:
        print("Forcing to collect")
        STM.Kaggle_process_per_minute_data(symbol_list = finals_symbols,last_date = missinng_saturday_date,weekday=None)
if today.weekday() == 0:
    print("there is monday today")
    yesterday = today - timedelta(days=1)
    yesterdays_date = yesterday.strftime('%Y-%m-%d 00:00:00')
    STM.Kaggle_process_per_minute_data(symbol_list = finals_symbols,last_date = yesterdays_date,weekday = 0)
else:
    print("there is no monday today")
STM.update_stocks_list_for_today()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
print('Creating metadata file for daily data>>>>')
data = {
    "id": "ayushkhaire/stock-past-one-year-data"
}
metadata_file_location = '/kaggle/working/daily_update_to_kaggle_final/dataset-metadata.json' 
with open(metadata_file_location, 'w', encoding='utf-8') as metadata_file:
    json.dump(data, metadata_file)
print('Metadata file created for daily data')

if today.weekday() == 0 or force == True:
    print('Creating metadata file for per minute data>>>>')
    data = {
        "id": "ayushkhaire/real-time-stocks-data"
    }
    metadata_file_location = '/kaggle/working/per_minute_to_kaggle_final/dataset-metadata.json' 
    with open(metadata_file_location, 'w', encoding='utf-8') as metadata_file:
        json.dump(data, metadata_file)
    print('Metadata file created for per minute data')
else:
    print("there is no monday today")
    
os.environ['KAGGLE_USERNAME'] = kaggle_username
os.environ['KAGGLE_KEY'] = kaggle_apikey
retries = 0
while retries < 5:
    try:
        command = "kaggle datasets version -p '/kaggle/working/daily_update_to_kaggle_final' -m 'Update' -r zip"
        subprocess.run(command, shell=True, check=True)
        logger.info("Upload completefor daily data")
        break
    except Exception as error:
        logger.error(f"Error from Kaggle: {error}")
        time.sleep(5)
        retries += 1
if today.weekday() == 0 or force == True:
    print("there is monday today")    
    retries = 0
    while retries < 5:
        try:
            command = "kaggle datasets version -p '/kaggle/working/per_minute_to_kaggle_final' -m 'Update' -r zip"
            subprocess.run(command, shell=True, check=True)
            logger.info("Upload completefor per minute data")
            break
        except Exception as error:
            logger.error(f"Error from Kaggle: {error}")
            time.sleep(5)
            retries += 1
else:
    print("there is no saturday today")    
    
RCC = recoverData()
RCC.setup()
targets = RCC.detect_all_stocks()
RCC.download_bunches_mass(targets)
newcollecteddf = RCC.merge_new_data()
RCC.final_merge(newcollecteddf)
RCC.create_metadata_to_push_recover()
RCC.upload_recovered_to_kaggle()
shutil.rmtree('/kaggle/working/daily_update')
shutil.rmtree('/kaggle/working/daily_update_to_kaggle')
if today.weekday() == 0 or force == True:
    shutil.rmtree('/kaggle/working/per_minute')
    shutil.rmtree('/kaggle/working/per_minute_to_kaggle')