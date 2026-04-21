"""
Kafka Producer - Real-time Transaction Streaming
Simulates real-time payment transactions and sends to Kafka
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
from faker import Faker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class TransactionProducer:
    """Produces real-time payment transactions to Kafka"""
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic = 'payment-transactions'
        
    def generate_transaction(self):
        """Generate a single transaction"""
        transaction_status = random.choices(
            ['success', 'failed', 'pending'],
            weights=[85, 10, 5],
            k=1
        )[0]
        
        payment_types = ['credit_card', 'debit_card', 'wallet', 'upi', 'net_banking']
        
        transaction = {
            'transaction_id': fake.uuid4(),
            'merchant_id': random.randint(1, 50),
            'customer_id': random.randint(1, 1000),
            'amount': round(random.uniform(10, 2000), 2),
            'currency': 'USD',
            'payment_type': random.choice(payment_types),
            'transaction_status': transaction_status,
            'customer_ip': fake.ipv4(),
            'timestamp': datetime.now().isoformat(),
            'gateway_id': random.randint(1, 4),
            'metadata': {
                'device_type': random.choice(['mobile', 'desktop', 'tablet']),
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
            }
        }
        
        return transaction
    
    def produce_transactions(self, count=100, interval=1):
        """Produce multiple transactions with time interval"""
        logger.info(f"Starting to produce {count} transactions...")
        
        for i in range(count):
            transaction = self.generate_transaction()
            
            # Use customer_id as key for partitioning
            key = f"customer_{transaction['customer_id']}"
            
            self.producer.send(
                self.topic,
                key=key,
                value=transaction
            )
            
            logger.info(f"Produced transaction {i+1}/{count}: {transaction['transaction_id']} - {transaction['transaction_status']}")
            
            time.sleep(interval)
        
        self.producer.flush()
        logger.info("All transactions produced successfully!")
    
    def close(self):
        """Close the producer"""
        self.producer.close()


if __name__ == "__main__":
    producer = TransactionProducer()
    
    try:
        # Produce 100 transactions with 1 second interval
        producer.produce_transactions(count=100, interval=1)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.close()
