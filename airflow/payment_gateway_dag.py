"""
Airflow DAG for Payment Gateway ETL Pipeline
Orchestrates data generation, ETL, and quality checks
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_generator import PaymentDataGenerator
from etl_pipeline import PaymentGatewayETL
from data_quality import DataQualityChecker
import pandas as pd


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email': ['alerts@paymentgateway.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def generate_data(**context):
    """Task: Generate payment gateway data"""
    print("Starting data generation...")
    generator = PaymentDataGenerator(
        num_merchants=50,
        num_customers=1000,
        num_transactions=10000
    )
    data = generator.generate_all_data()
    print(f"Data generation completed: {len(data['transactions'])} transactions")
    return True


def run_etl(**context):
    """Task: Run ETL pipeline"""
    print("Starting ETL pipeline...")
    etl = PaymentGatewayETL()
    success = etl.run_etl()
    
    if not success:
        raise Exception("ETL pipeline failed")
    
    print("ETL pipeline completed successfully")
    return True


def validate_data(**context):
    """Task: Validate data quality"""
    print("Starting data quality validation...")
    
    # Load processed data
    data = {
        'merchants': pd.read_csv('data/processed/merchants_processed.csv'),
        'customers': pd.read_csv('data/processed/customers_processed.csv'),
        'payment_methods': pd.read_csv('data/processed/payment_methods_processed.csv'),
        'transactions': pd.read_csv('data/processed/transactions_processed.csv'),
        'fraud_alerts': pd.read_csv('data/processed/fraud_alerts_processed.csv'),
        'refunds': pd.read_csv('data/processed/refunds_processed.csv')
    }
    
    checker = DataQualityChecker()
    is_valid = checker.validate_all(data)
    
    if not is_valid:
        raise Exception("Data quality validation failed")
    
    print("Data quality validation completed successfully")
    return True


def calculate_metrics(**context):
    """Task: Calculate and log business metrics"""
    print("Calculating business metrics...")
    
    transactions = pd.read_csv('data/processed/transactions_processed.csv')
    
    metrics = {
        'total_transactions': len(transactions),
        'total_revenue': transactions[transactions['transaction_status'] == 'success']['amount'].sum(),
        'success_rate': (transactions['transaction_status'] == 'success').mean() * 100,
        'avg_transaction_value': transactions[transactions['transaction_status'] == 'success']['amount'].mean()
    }
    
    print("Business Metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value:.2f}")
    
    # Push metrics to XCom for downstream tasks
    context['ti'].xcom_push(key='metrics', value=metrics)
    return metrics


def send_summary_report(**context):
    """Task: Generate and send summary report"""
    print("Generating summary report...")
    
    # Pull metrics from previous task
    ti = context['ti']
    metrics = ti.xcom_pull(task_ids='calculate_metrics', key='metrics')
    
    report = f"""
    ========================================
    Payment Gateway Pipeline Summary Report
    ========================================
    Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    Pipeline Status: SUCCESS ✓
    
    Key Metrics:
    - Total Transactions: {metrics['total_transactions']:,}
    - Total Revenue: ${metrics['total_revenue']:,.2f}
    - Success Rate: {metrics['success_rate']:.2f}%
    - Average Transaction: ${metrics['avg_transaction_value']:.2f}
    
    ========================================
    """
    
    print(report)
    
    # In production, send this via email or Slack
    return True


# Define the DAG
dag = DAG(
    'payment_gateway_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline for payment gateway data',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['payment', 'etl', 'data-engineering'],
)


# Define tasks
task_generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    dag=dag,
)

task_run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

task_validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

task_calculate_metrics = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics,
    dag=dag,
)

task_backup_data = BashOperator(
    task_id='backup_data',
    bash_command='tar -czf data/backups/backup_{{ ds }}.tar.gz data/processed/',
    dag=dag,
)

task_send_report = PythonOperator(
    task_id='send_summary_report',
    python_callable=send_summary_report,
    dag=dag,
)


# Define task dependencies
task_generate_data >> task_run_etl >> task_validate_data >> task_calculate_metrics
task_calculate_metrics >> [task_backup_data, task_send_report]


# Task documentation
task_generate_data.doc_md = """
### Generate Data Task
Generates synthetic payment gateway transaction data including:
- Merchants
- Customers
- Payment methods
- Transactions
- Fraud alerts
- Refunds
"""

task_run_etl.doc_md = """
### ETL Pipeline Task
Executes the complete ETL process:
1. Extract: Read data from CSV files
2. Transform: Clean, validate, and enrich data
3. Load: Save processed data and optionally load to database
"""

task_validate_data.doc_md = """
### Data Quality Task
Performs comprehensive data quality checks:
- Null value validation
- Duplicate detection
- Data type verification
- Referential integrity
- Business rule validation
- Anomaly detection
"""
