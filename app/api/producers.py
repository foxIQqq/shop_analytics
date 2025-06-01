import os
import json
import logging
from api.config import settings

logger = logging.getLogger(__name__)

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)

try:
    from kafka import KafkaProducer
    
    class KafkaProducerSingleton:
        _instance = None
        
        @classmethod
        def get_instance(cls):
            if cls._instance is None:
                try:
                    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
                    logger.info(f"Connecting to Kafka at {bootstrap_servers}")
                    cls._instance = KafkaProducer(
                        bootstrap_servers=bootstrap_servers,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        acks='all',
                        retries=3
                    )
                    logger.info("Successfully connected to Kafka")
                except Exception as e:
                    logger.error(f"Failed to connect to Kafka: {str(e)}")
                    # Создаем заглушку, чтобы не падало приложение при отсутствии Kafka
                    cls._instance = None
            return cls._instance
except ImportError:
    logger.warning("Kafka modules not available, using dummy implementation")
    
    class KafkaProducerSingleton:
        _instance = None
        
        @classmethod
        def get_instance(cls):
            logger.warning("Using dummy Kafka producer")
            return None

async def send_product(product_data: dict):
    producer = KafkaProducerSingleton.get_instance()
    try:
        if producer:
            producer.send(settings.KAFKA_PRODUCTS_TOPIC, value=product_data)
            logger.info(f"Sent product data to Kafka: {product_data.get('id', 'new')}")
        else:
            logger.warning(f"Kafka not available, product data not sent: {product_data.get('id', 'new')}")
    except Exception as e:
        logger.error(f"Failed to send product data: {str(e)}")

async def send_seller(seller_data: dict):
    producer = KafkaProducerSingleton.get_instance()
    try:
        if producer:
            producer.send(settings.KAFKA_SELLERS_TOPIC, value=seller_data)
            logger.info(f"Sent seller data to Kafka: {seller_data.get('id', 'new')}")
        else:
            logger.warning(f"Kafka not available, seller data not sent: {seller_data.get('id', 'new')}")
    except Exception as e:
        logger.error(f"Failed to send seller data: {str(e)}")

async def send_purchase(purchase_data: dict):
    producer = KafkaProducerSingleton.get_instance()
    try:
        if producer:
            producer.send(settings.KAFKA_PURCHASES_TOPIC, value=purchase_data)
            logger.info(f"Sent purchase data to Kafka: {purchase_data.get('id', 'new')}")
        else:
            logger.warning(f"Kafka not available, purchase data not sent: {purchase_data.get('id', 'new')}")
    except Exception as e:
        logger.error(f"Failed to send purchase data: {str(e)}")

async def send_customer(customer_data: dict):
    producer = KafkaProducerSingleton.get_instance()
    try:
        if producer:
            producer.send(settings.KAFKA_CUSTOMERS_TOPIC, value=customer_data)
            logger.info(f"Sent customer data to Kafka: {customer_data.get('customer_id', 'new')}")
        else:
            logger.warning(f"Kafka not available, customer data not sent: {customer_data.get('customer_id', 'new')}")
    except Exception as e:
        logger.error(f"Failed to send customer data: {str(e)}")