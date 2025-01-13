import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from logger_config import logger
import gc
from tqdm import tqdm

# Load environment variables
load_dotenv(dotenv_path='../.env')

mysql_username = os.getenv('MYSQL_USERNAME')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_broker = os.getenv('MYSQL_BROKER')
mysql_database_name = os.getenv('MYSQL_DATABASE_NAME')

# Database connection
cnx = None
try:
    cnx = mysql.connector.connect(
        user=mysql_username,
        password=mysql_password,
        database=mysql_database_name,
        host=mysql_broker
    )
    logger.debug("Database connected")
except mysql.connector.Error as err:
    logger.error(err)
    exit(1)

cursor = cnx.cursor()

TABLES = {
    'real_time_data': '''
    CREATE TABLE real_time_data (
      stockname VARCHAR(100) NOT NULL,
      timestamp VARCHAR(100) NOT NULL, 
      `open` FLOAT,
      `high` FLOAT,
      `low` FLOAT,
      `close` FLOAT
    );
    '''
}

def create_table(table_name, drop=False):
    if drop:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            logger.critical(f"Table `{table_name}` dropped.")
        except mysql.connector.Error as err:
            logger.error(f"Error dropping table `{table_name}`: {err}")
    try:
        cursor.execute(TABLES[table_name])
        logger.debug(f"Created table `{table_name}` successfully.")
    except mysql.connector.Error as err:
        logger.error(f"Error creating table `{table_name}`: {err}")

def feed_data_to_table(table_name, df):
    for _, row in tqdm(
      df.iterrows(), 
      desc="Inserting rows", 
      total=len(df)
    ):
        query = f"""
        INSERT INTO {table_name} (stockname, timestamp, `open`, `high`, `low`, `close`)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        values = (row['stockname'], row['timestamp'], row['open'], row['high'], row['low'], row['close'])
        try:
            cursor.execute(query, values)
            cnx.commit()
            # logger.info(f"Inserted row: {values}")
        except mysql.connector.Error as err:
            logger.error(f"Error inserting data into `{table_name}`: {err}")

# Create the table
create_table('real_time_data', drop=True)

# Load and insert the first 100 rows from the CSV
df = pd.read_csv("source/stocks.csv") 
feed_data_to_table('real_time_data', df)

del df 
gc.collect()
logger.warning("memory cleaned")

# Close connection
cursor.close()
cnx.close()
logger.debug("Database connection closed.")
