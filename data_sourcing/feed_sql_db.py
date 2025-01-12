import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode
from logger_config import logger

load_dotenv(dotenv_path='../.env')

mysql_username = os.getenv('MYSQL_USERNAME')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_broker = os.getenv('MYSQL_BROKER')
mysql_database_name = os.getenv('MYSQL_DATABASE_NAME')

try:
  cnx = mysql.connector.connect(
      user=mysql_username,
      password = mysql_password,
      database=mysql_database_name,
      host = mysql_broker
    )
  logger.debug("database connected")
  
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    logger.error(err)
else:
  cnx.close()