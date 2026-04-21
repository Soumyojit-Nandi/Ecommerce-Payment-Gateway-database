"""
Kafka Consumer - Real-time Transaction Processing
Consumes transactions from Kafka and stores in PostgreSQL
"""

from kafka import KafkaConsumer
import json
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionConsumer:
    """Consumes transactions from Kafka and processes them"""
    
    def __init__(self, bootstrap_servers='localhost:9092', db_conn_string=None):
        self.consumer = KafkaConsumer(
            'payment-transactions',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='transaction-processor-group'
        )
        
        # Database connection
        if db_conn_string:
            self.engine = create_engine(db_conn_string)
        else:
            self.engine = create_engine(
                'postgresql://postgres:postgres@localhost:5432/payment_gateway_db'
            )
    
    def process_transaction(self, transaction):
        """Process and store transaction in database"""
        try:
            with self.engine.connect() as conn:
                # Insert transaction into database
                query = text("""
                    INSERT INTO transactions (
                        transaction_uuid, merchant_id, customer_id, 
                        payment_method_id, gateway_id, amount, currency,
                        transaction_status, payment_type, customer_ip,
                        transaction_timestamp, metadata
                    ) VALUES (
                        :transaction_id, :merchant_id, :customer_id,
                        1, :gateway_id, :amount, :currency,
                        :transaction_status, :payment_type, :customer_ip,
                        :timestamp, :metadata
                    )
                """)
                
                conn.execute(query, {
                    'transaction_id': transaction['transaction_id'],
                    'merchant_id': transaction['merchant_id'],
                    'customer_id': transaction['customer_id'],
                    'gateway_id': transaction['gateway_id'],
                    'amount': transaction['amount'],
                    'currency': transaction['currency'],
                    'transaction_status': transaction['transaction_status'],
                    'payment_type': transaction['payment_type'],
                    'customer_ip': transaction['customer_ip'],
                    'timestamp': transaction['timestamp'],
                    'metadata': json.dumps(transaction.get('metadata', {}))
                })
                conn.commit()
                
                logger.info(f"Processed transaction: {transaction['transaction_id']}")
                
                # Fraud detection check
                if transaction['amount'] > 1500:
                    self.create_fraud_alert(conn, transaction)
                    
        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}")
    
    def create_fraud_alert(self, conn, transaction):
        """Create fraud alert for suspicious transactions"""
        try:
            query = text("""
                INSERT INTO fraud_alerts (
                    transaction_id, alert_type, risk_level,
                    alert_message, created_at
                )
                SELECT 
                    t.transaction_id,
                    'High value transaction',
                    'high',
                    'Transaction amount exceeds threshold',
                    NOW()
                FROM transactions t
                WHERE t.transaction_uuid = :transaction_uuid
            """)
            
            conn.execute(query, {'transaction_uuid': transaction['transaction_id']})
            conn.commit()
            
            logger.warning(f"Fraud alert created for transaction: {transaction['transaction_id']}")
        except Exception as e:
            logger.error(f"Error creating fraud alert: {str(e)}")
    
    def consume_transactions(self):
        """Start consuming transactions from Kafka"""
        logger.info("Starting transaction consumer...")
        
        try:
            for message in self.consumer:
                transaction = message.value
                logger.info(f"Received transaction: {transaction['transaction_id']}")
                self.process_transaction(transaction)
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = TransactionConsumer()
    consumer.consume_transactions()
