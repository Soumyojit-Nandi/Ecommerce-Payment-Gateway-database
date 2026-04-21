"""
ETL Pipeline for Payment Gateway Data
Extracts data from CSV, transforms it, and loads into PostgreSQL database
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import yaml
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaymentGatewayETL:
    """ETL Pipeline for Payment Gateway Data"""
    
    def __init__(self, config_path='config/config.yaml'):
        """Initialize ETL pipeline with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.db_config = self.config['database']
        self.engine = None
        
    def connect_db(self):
        """Create database connection"""
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string)
            logger.info("Database connection established successfully")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            return False
    
    def extract_data(self, data_dir='data/raw'):
        """Extract data from CSV files"""
        logger.info("Starting data extraction...")
        
        try:
            data = {
                'merchants': pd.read_csv(f'{data_dir}/merchants.csv'),
                'customers': pd.read_csv(f'{data_dir}/customers.csv'),
                'payment_methods': pd.read_csv(f'{data_dir}/payment_methods.csv'),
                'transactions': pd.read_csv(f'{data_dir}/transactions.csv'),
                'fraud_alerts': pd.read_csv(f'{data_dir}/fraud_alerts.csv'),
                'refunds': pd.read_csv(f'{data_dir}/refunds.csv')
            }
            
            logger.info("Data extraction completed successfully")
            for table, df in data.items():
                logger.info(f"  - {table}: {len(df)} records")
            
            return data
        except Exception as e:
            logger.error(f"Data extraction failed: {str(e)}")
            return None
    
    def transform_merchants(self, df):
        """Transform merchants data"""
        logger.info("Transforming merchants data...")
        
        # Clean merchant names
        df['merchant_name'] = df['merchant_name'].str.strip()
        df['merchant_email'] = df['merchant_email'].str.lower()
        
        # Ensure commission rate is within valid range
        df['commission_rate'] = df['commission_rate'].clip(0, 10)
        
        # Convert dates
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['updated_at'] = pd.to_datetime(df['created_at'])  # Same as created initially
        
        return df
    
    def transform_customers(self, df):
        """Transform customers data"""
        logger.info("Transforming customers data...")
        
        # Clean emails and names
        df['customer_email'] = df['customer_email'].str.lower().str.strip()
        df['customer_name'] = df['customer_name'].str.strip()
        
        # Ensure risk score is between 0-100
        df['risk_score'] = df['risk_score'].clip(0, 100)
        
        # Convert dates
        df['customer_since'] = pd.to_datetime(df['customer_since'])
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Handle missing values
        df['phone_number'] = df['phone_number'].fillna('Not provided')
        
        return df
    
    def transform_transactions(self, df):
        """Transform transactions data"""
        logger.info("Transforming transactions data...")
        
        # Convert timestamps
        df['transaction_timestamp'] = pd.to_datetime(df['transaction_timestamp'])
        df['processed_timestamp'] = pd.to_datetime(df['processed_timestamp'], errors='coerce')
        
        # Ensure amount is positive
        df['amount'] = df['amount'].abs()
        
        # Clean status values
        df['transaction_status'] = df['transaction_status'].str.lower().str.strip()
        
        # Create derived fields
        df['transaction_hour'] = df['transaction_timestamp'].dt.hour
        df['transaction_day_of_week'] = df['transaction_timestamp'].dt.dayofweek
        df['transaction_month'] = df['transaction_timestamp'].dt.month
        
        # Processing time in seconds
        df['processing_time_seconds'] = (
            df['processed_timestamp'] - df['transaction_timestamp']
        ).dt.total_seconds()
        
        return df
    
    def transform_payment_methods(self, df):
        """Transform payment methods data"""
        logger.info("Transforming payment methods data...")
        
        # Convert dates
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Fill NaN values for card fields
        df['card_last_4'] = df['card_last_4'].fillna('')
        df['card_brand'] = df['card_brand'].fillna('')
        
        return df
    
    def transform_fraud_alerts(self, df):
        """Transform fraud alerts data"""
        logger.info("Transforming fraud alerts data...")
        
        # Convert timestamps
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')
        
        # Standardize risk levels
        df['risk_level'] = df['risk_level'].str.lower()
        
        # Fill resolved_by for unresolved alerts
        df['resolved_by'] = df['resolved_by'].fillna('Pending')
        
        return df
    
    def transform_refunds(self, df):
        """Transform refunds data"""
        logger.info("Transforming refunds data...")
        
        # Convert timestamps
        df['requested_at'] = pd.to_datetime(df['requested_at'])
        df['processed_at'] = pd.to_datetime(df['processed_at'], errors='coerce')
        
        # Ensure refund amount is positive
        df['refund_amount'] = df['refund_amount'].abs()
        
        # Standardize status
        df['refund_status'] = df['refund_status'].str.lower()
        
        return df
    
    def apply_transformations(self, data):
        """Apply all transformations"""
        logger.info("Applying data transformations...")
        
        transformed_data = {
            'merchants': self.transform_merchants(data['merchants']),
            'customers': self.transform_customers(data['customers']),
            'payment_methods': self.transform_payment_methods(data['payment_methods']),
            'transactions': self.transform_transactions(data['transactions']),
            'fraud_alerts': self.transform_fraud_alerts(data['fraud_alerts']),
            'refunds': self.transform_refunds(data['refunds'])
        }
        
        logger.info("Data transformation completed")
        return transformed_data
    
    def calculate_aggregates(self, transactions_df):
        """Calculate aggregate metrics"""
        logger.info("Calculating aggregate metrics...")
        
        aggregates = {
            'total_transactions': len(transactions_df),
            'total_revenue': transactions_df[transactions_df['transaction_status'] == 'success']['amount'].sum(),
            'avg_transaction_value': transactions_df[transactions_df['transaction_status'] == 'success']['amount'].mean(),
            'success_rate': (transactions_df['transaction_status'] == 'success').mean() * 100,
            'total_refunded': transactions_df[transactions_df['transaction_status'] == 'refunded']['amount'].sum()
        }
        
        logger.info("Aggregate metrics calculated:")
        for key, value in aggregates.items():
            if isinstance(value, float):
                logger.info(f"  - {key}: {value:.2f}")
            else:
                logger.info(f"  - {key}: {value}")
        
        return aggregates
    
    def load_data(self, data, if_exists='replace'):
        """Load transformed data into PostgreSQL"""
        logger.info("Starting data loading to database...")
        
        if not self.engine:
            logger.error("Database connection not established")
            return False
        
        try:
            # Define load order based on foreign key dependencies
            load_order = [
                'merchants',
                'customers',
                'payment_methods',
                'transactions',
                'fraud_alerts',
                'refunds'
            ]
            
            for table_name in load_order:
                df = data[table_name]
                
                # Drop derived columns that aren't in the database schema
                if table_name == 'transactions':
                    columns_to_drop = ['transaction_hour', 'transaction_day_of_week', 
                                     'transaction_month', 'processing_time_seconds']
                    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
                
                # Load to database
                df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
                logger.info(f"  - Loaded {len(df)} records to {table_name}")
            
            logger.info("Data loading completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            return False
    
    def save_processed_data(self, data, output_dir='data/processed'):
        """Save processed data to CSV for backup"""
        logger.info("Saving processed data to CSV...")
        
        os.makedirs(output_dir, exist_ok=True)
        
        for table_name, df in data.items():
            output_path = f'{output_dir}/{table_name}_processed.csv'
            df.to_csv(output_path, index=False)
            logger.info(f"  - Saved {table_name} to {output_path}")
        
        logger.info("Processed data saved successfully")
    
    def run_etl(self):
        """Execute complete ETL pipeline"""
        logger.info("=" * 50)
        logger.info("Starting Payment Gateway ETL Pipeline")
        logger.info("=" * 50)
        
        start_time = datetime.now()
        
        # Extract
        raw_data = self.extract_data()
        if not raw_data:
            logger.error("ETL pipeline failed at extraction stage")
            return False
        
        # Transform
        transformed_data = self.apply_transformations(raw_data)
        
        # Calculate aggregates
        aggregates = self.calculate_aggregates(transformed_data['transactions'])
        
        # Save processed data
        self.save_processed_data(transformed_data)
        
        # Load (commented out if no database available)
        # Uncomment these lines when database is configured
        # if self.connect_db():
        #     self.load_data(transformed_data)
        # else:
        #     logger.warning("Skipping database load due to connection failure")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 50)
        logger.info(f"ETL Pipeline completed successfully in {duration:.2f} seconds")
        logger.info("=" * 50)
        
        return True


if __name__ == "__main__":
    etl = PaymentGatewayETL()
    etl.run_etl()
