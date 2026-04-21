"""
Advanced Airflow DAG - Integrated Payment Gateway Pipeline
Orchestrates Kafka, Spark, and PostgreSQL workflows
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Default arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email': ['alerts@paymentgateway.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def start_kafka_producer(**context):
    """Start Kafka producer to generate streaming transactions"""
    print("Starting Kafka producer for real-time transactions...")
    import subprocess
    subprocess.run([
        'python', '/opt/airflow/src/kafka_producer.py'
    ], check=True)
    print("Kafka producer completed")


def validate_kafka_stream(**context):
    """Validate Kafka stream is running"""
    print("Validating Kafka stream...")
    from kafka import KafkaConsumer
    
    try:
        consumer = KafkaConsumer(
            'payment-transactions',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        message_count = 0
        for message in consumer:
            message_count += 1
            if message_count >= 10:
                break
        
        consumer.close()
        print(f"Validated {message_count} messages in Kafka stream")
        
        if message_count == 0:
            raise Exception("No messages found in Kafka stream!")
            
    except Exception as e:
        print(f"Kafka validation failed: {str(e)}")
        raise


def run_spark_batch_analytics(**context):
    """Run Spark batch analytics on transactions"""
    print("Running Spark batch analytics...")
    import subprocess
    
    subprocess.run([
        'spark-submit',
        '--master', 'spark://spark-master:7077',
        '--deploy-mode', 'client',
        '/opt/airflow/src/spark_analytics.py'
    ], check=True)
    
    print("Spark analytics completed")


def generate_batch_data(**context):
    """Generate batch transaction data"""
    print("Generating batch transaction data...")
    sys.path.insert(0, '/opt/airflow/src')
    from data_generator import PaymentDataGenerator
    
    generator = PaymentDataGenerator(
        num_merchants=50,
        num_customers=1000,
        num_transactions=5000
    )
    
    data = generator.generate_all_data(output_dir='/opt/airflow/data/raw')
    print(f"Generated {len(data['transactions'])} transactions")


def load_to_postgres(**context):
    """Load processed data to PostgreSQL"""
    print("Loading data to PostgreSQL...")
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine('postgresql://postgres:postgres@postgres:5432/payment_gateway_db')
    
    # Load CSV files
    transactions = pd.read_csv('/opt/airflow/data/raw/transactions.csv')
    merchants = pd.read_csv('/opt/airflow/data/raw/merchants.csv')
    customers = pd.read_csv('/opt/airflow/data/raw/customers.csv')
    
    # Load to database
    merchants.to_sql('merchants', engine, if_exists='append', index=False)
    customers.to_sql('customers', engine, if_exists='append', index=False)
    transactions.to_sql('transactions', engine, if_exists='append', index=False)
    
    print("Data loaded to PostgreSQL successfully")


def generate_analytics_report(**context):
    """Generate final analytics report"""
    print("Generating analytics report...")
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine('postgresql://postgres:postgres@postgres:5432/payment_gateway_db')
    
    # Run analytics query
    query = """
        SELECT 
            DATE(transaction_timestamp) as date,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN transaction_status = 'success' THEN amount ELSE 0 END) as revenue,
            AVG(amount) as avg_transaction
        FROM transactions
        WHERE transaction_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY DATE(transaction_timestamp)
        ORDER BY date DESC
    """
    
    df = pd.read_sql(query, engine)
    
    print("\n=== Weekly Analytics Report ===")
    print(df.to_string())
    print("\nTotal Revenue:", df['revenue'].sum())
    print("Average Daily Transactions:", df['total_transactions'].mean())


# Define the DAG
dag = DAG(
    'payment_gateway_advanced_pipeline',
    default_args=default_args,
    description='Advanced pipeline with Kafka, Spark, PostgreSQL integration',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['payment', 'kafka', 'spark', 'real-time'],
)


# Define tasks
task_generate_batch = PythonOperator(
    task_id='generate_batch_data',
    python_callable=generate_batch_data,
    dag=dag,
)

task_start_kafka = PythonOperator(
    task_id='start_kafka_producer',
    python_callable=start_kafka_producer,
    dag=dag,
)

task_validate_kafka = PythonOperator(
    task_id='validate_kafka_stream',
    python_callable=validate_kafka_stream,
    dag=dag,
)

task_load_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

task_spark_analytics = PythonOperator(
    task_id='run_spark_analytics',
    python_callable=run_spark_batch_analytics,
    dag=dag,
)

task_generate_report = PythonOperator(
    task_id='generate_analytics_report',
    python_callable=generate_analytics_report,
    dag=dag,
)

# Alternative: Use SparkSubmitOperator for more control
# task_spark_submit = SparkSubmitOperator(
#     task_id='spark_submit_analytics',
#     application='/opt/airflow/src/spark_analytics.py',
#     conn_id='spark_default',
#     dag=dag,
# )

# Define task dependencies
task_generate_batch >> task_load_postgres
task_start_kafka >> task_validate_kafka
[task_load_postgres, task_validate_kafka] >> task_spark_analytics >> task_generate_report


# Task documentation
task_generate_batch.doc_md = """
### Generate Batch Data
Generates synthetic payment transaction data including merchants, customers, and transactions.
This data is then loaded into PostgreSQL for batch processing.
"""

task_start_kafka.doc_md = """
### Start Kafka Producer
Starts a Kafka producer that simulates real-time payment transactions.
Transactions are streamed to the 'payment-transactions' topic.
"""

task_spark_analytics.doc_md = """
### Spark Analytics
Runs large-scale analytics on transaction data using Apache Spark:
- Transaction success rate analysis
- Hourly pattern detection
- Fraud pattern identification
- Customer lifetime value calculation
"""
