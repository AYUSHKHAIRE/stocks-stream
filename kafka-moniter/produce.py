# produce data

from confluent_kafka import Producer
import os
from dotenv import load_dotenv
from logger_config import logger

load_dotenv(dotenv_path='../.env')

# Kafka configuration
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_port = os.getenv('KAFKA_PORT_NO')
kafka_producer_topic = os.getenv('KAFKA_CONSUMER_TOPIC_NAME')

conf = {
    'bootstrap.servers': f'{kafka_broker}:{kafka_port}'
}

logger.debug(f"SETTING UP | KAFKA |  PRODUCER | {kafka_broker}:{kafka_port} >  {kafka_producer_topic}")

try:
    producer = Producer(conf)
    message = "producer working correctly ."
    producer.produce(kafka_producer_topic, value=str(message))
    producer.flush()
    logger.debug(f"produced test message : {message}")
except Exception as e:
    logger.errpr(f"got an ERROR while producing in {kafka_broker}:{kafka_port} >  {kafka_producer_topic}")
    logger.error(e)
