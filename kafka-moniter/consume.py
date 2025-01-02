# consume kafka

from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
from logger_config import logger

load_dotenv(dotenv_path='../.env')

kafka_broker = os.getenv('KAFKA_BROKER')
kafka_port = os.getenv('KAFKA_PORT_NO')
kafka_consumer_topic = os.getenv('KAFKA_CONSUMER_TOPIC_NAME')
kafka_group_id = os.getenv('KAFKA_CONSUMER_GROUP_ID')  

conf = {
    'bootstrap.servers': f'{kafka_broker}:{kafka_port}',
    'group.id': kafka_group_id, 
    'auto.offset.reset': 'earliest'  
}

consumer = Consumer(conf)

consumer.subscribe([kafka_consumer_topic])

logger.debug(f"SETTING UP | KAFKA |  CONSUMER | {kafka_broker}:{kafka_port} >  {kafka_consumer_topic}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)  
        
        if msg is None:
            continue 
        if msg.error():
            logger.error("ERROR in consuming message")
            logger.error(msg.error())
            continue
        
        logger.info(f"Consumed message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    logger.warning("Consumer stopped.")

finally:
    consumer.close()
