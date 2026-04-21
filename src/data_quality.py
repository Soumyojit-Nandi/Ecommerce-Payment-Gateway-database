"""
Data Quality Checks for Payment Gateway Data
Implements validation rules and anomaly detection
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Comprehensive data quality validation"""
    
    def __init__(self):
        self.validation_results = []
        self.errors = []
        self.warnings = []
    
    def check_nulls(self, df, table_name, required_columns):
        """Check for null values in required columns"""
        logger.info(f"Checking null values in {table_name}...")
        
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    self.errors.append(f"{table_name}.{col}: {null_count} null values found")
                    logger.warning(f"  ⚠ {col}: {null_count} null values")
                else:
                    logger.info(f"  ✓ {col}: No null values")
    
    def check_duplicates(self, df, table_name, unique_columns):
        """Check for duplicate values"""
        logger.info(f"Checking duplicates in {table_name}...")
        
        for col in unique_columns:
            if col in df.columns:
                dup_count = df[col].duplicated().sum()
                if dup_count > 0:
                    self.errors.append(f"{table_name}.{col}: {dup_count} duplicate values")
                    logger.warning(f"  ⚠ {col}: {dup_count} duplicates")
                else:
                    logger.info(f"  ✓ {col}: No duplicates")
    
    def check_data_types(self, df, table_name, type_specs):
        """Validate data types"""
        logger.info(f"Checking data types in {table_name}...")
        
        for col, expected_type in type_specs.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if expected_type not in actual_type:
                    self.warnings.append(
                        f"{table_name}.{col}: Expected {expected_type}, got {actual_type}"
                    )
                    logger.warning(f"  ⚠ {col}: Type mismatch")
                else:
                    logger.info(f"  ✓ {col}: Correct type")
    
    def check_value_ranges(self, df, table_name, range_specs):
        """Check if values are within expected ranges"""
        logger.info(f"Checking value ranges in {table_name}...")
        
        for col, (min_val, max_val) in range_specs.items():
            if col in df.columns:
                out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
                if len(out_of_range) > 0:
                    self.warnings.append(
                        f"{table_name}.{col}: {len(out_of_range)} values out of range [{min_val}, {max_val}]"
                    )
                    logger.warning(f"  ⚠ {col}: {len(out_of_range)} out of range")
                else:
                    logger.info(f"  ✓ {col}: All values in range")
    
    def check_referential_integrity(self, child_df, parent_df, child_col, parent_col, relationship_name):
        """Check foreign key relationships"""
        logger.info(f"Checking referential integrity: {relationship_name}...")
        
        child_values = set(child_df[child_col].dropna().unique())
        parent_values = set(parent_df[parent_col].dropna().unique())
        
        orphaned = child_values - parent_values
        
        if orphaned:
            self.errors.append(
                f"{relationship_name}: {len(orphaned)} orphaned records"
            )
            logger.warning(f"  ⚠ Found {len(orphaned)} orphaned records")
        else:
            logger.info(f"  ✓ Referential integrity maintained")
    
    def check_business_rules(self, data):
        """Validate business-specific rules"""
        logger.info("Checking business rules...")
        
        # Rule 1: Transaction amounts should be positive
        negative_amounts = data['transactions'][data['transactions']['amount'] <= 0]
        if len(negative_amounts) > 0:
            self.errors.append(f"Found {len(negative_amounts)} transactions with non-positive amounts")
        
        # Rule 2: Refund amount should not exceed original transaction
        refunds_df = data['refunds'].merge(
            data['transactions'][['transaction_id', 'amount']],
            on='transaction_id'
        )
        excessive_refunds = refunds_df[refunds_df['refund_amount'] > refunds_df['amount']]
        if len(excessive_refunds) > 0:
            self.errors.append(f"Found {len(excessive_refunds)} refunds exceeding original amount")
        
        # Rule 3: Transaction timestamp should not be in the future
        future_transactions = data['transactions'][
            data['transactions']['transaction_timestamp'] > datetime.now()
        ]
        if len(future_transactions) > 0:
            self.errors.append(f"Found {len(future_transactions)} transactions with future timestamps")
        
        # Rule 4: Risk score should be between 0-100
        invalid_risk = data['customers'][
            (data['customers']['risk_score'] < 0) | (data['customers']['risk_score'] > 100)
        ]
        if len(invalid_risk) > 0:
            self.errors.append(f"Found {len(invalid_risk)} customers with invalid risk scores")
        
        # Rule 5: Successful transactions should have processed timestamp
        success_no_processed = data['transactions'][
            (data['transactions']['transaction_status'] == 'success') & 
            (data['transactions']['processed_timestamp'].isnull())
        ]
        if len(success_no_processed) > 0:
            self.warnings.append(
                f"Found {len(success_no_processed)} successful transactions without processed timestamp"
            )
        
        logger.info(f"Business rule checks completed")
    
    def detect_anomalies(self, df):
        """Detect statistical anomalies in transaction data"""
        logger.info("Detecting anomalies...")
        
        # Anomaly 1: Unusually high transaction amounts (> 3 standard deviations)
        amount_mean = df['amount'].mean()
        amount_std = df['amount'].std()
        threshold = amount_mean + (3 * amount_std)
        
        high_value_txns = df[df['amount'] > threshold]
        if len(high_value_txns) > 0:
            self.warnings.append(
                f"Found {len(high_value_txns)} transactions with unusually high amounts (>${threshold:.2f})"
            )
            logger.info(f"  → {len(high_value_txns)} high-value transactions detected")
        
        # Anomaly 2: Multiple failed transactions from same customer
        failed_by_customer = df[df['transaction_status'] == 'failed'].groupby('customer_id').size()
        suspicious_customers = failed_by_customer[failed_by_customer > 5]
        
        if len(suspicious_customers) > 0:
            self.warnings.append(
                f"Found {len(suspicious_customers)} customers with >5 failed transactions"
            )
            logger.info(f"  → {len(suspicious_customers)} suspicious customers")
        
        # Anomaly 3: Transactions outside business hours (12 AM - 6 AM)
        df['hour'] = pd.to_datetime(df['transaction_timestamp']).dt.hour
        odd_hour_txns = df[(df['hour'] >= 0) & (df['hour'] < 6)]
        
        if len(odd_hour_txns) > 0:
            logger.info(f"  → {len(odd_hour_txns)} transactions during odd hours (12 AM - 6 AM)")
        
        logger.info("Anomaly detection completed")
    
    def validate_all(self, data):
        """Run all validation checks"""
        logger.info("=" * 60)
        logger.info("Starting Data Quality Validation")
        logger.info("=" * 60)
        
        # Merchants validation
        self.check_nulls(
            data['merchants'], 'merchants',
            ['merchant_id', 'merchant_name', 'merchant_email']
        )
        self.check_duplicates(
            data['merchants'], 'merchants',
            ['merchant_id', 'merchant_email']
        )
        
        # Customers validation
        self.check_nulls(
            data['customers'], 'customers',
            ['customer_id', 'customer_email']
        )
        self.check_duplicates(
            data['customers'], 'customers',
            ['customer_id', 'customer_email']
        )
        self.check_value_ranges(
            data['customers'], 'customers',
            {'risk_score': (0, 100)}
        )
        
        # Transactions validation
        self.check_nulls(
            data['transactions'], 'transactions',
            ['transaction_id', 'transaction_uuid', 'amount', 'transaction_status']
        )
        self.check_duplicates(
            data['transactions'], 'transactions',
            ['transaction_id', 'transaction_uuid']
        )
        self.check_value_ranges(
            data['transactions'], 'transactions',
            {'amount': (0, 1000000)}
        )
        
        # Referential integrity checks
        self.check_referential_integrity(
            data['transactions'], data['merchants'],
            'merchant_id', 'merchant_id',
            'transactions -> merchants'
        )
        self.check_referential_integrity(
            data['transactions'], data['customers'],
            'customer_id', 'customer_id',
            'transactions -> customers'
        )
        self.check_referential_integrity(
            data['payment_methods'], data['customers'],
            'customer_id', 'customer_id',
            'payment_methods -> customers'
        )
        
        # Business rules
        self.check_business_rules(data)
        
        # Anomaly detection
        self.detect_anomalies(data['transactions'])
        
        # Summary
        self.print_summary()
        
        return len(self.errors) == 0
    
    def print_summary(self):
        """Print validation summary"""
        logger.info("=" * 60)
        logger.info("Data Quality Validation Summary")
        logger.info("=" * 60)
        logger.info(f"Total Errors: {len(self.errors)}")
        logger.info(f"Total Warnings: {len(self.warnings)}")
        
        if self.errors:
            logger.error("\nERRORS:")
            for error in self.errors:
                logger.error(f"  ❌ {error}")
        
        if self.warnings:
            logger.warning("\nWARNINGS:")
            for warning in self.warnings:
                logger.warning(f"  ⚠ {warning}")
        
        if not self.errors and not self.warnings:
            logger.info("\n✅ All data quality checks passed!")
        
        logger.info("=" * 60)


if __name__ == "__main__":
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
    
    if is_valid:
        print("\n✅ Data is ready for production use")
    else:
        print("\n❌ Data quality issues detected - please review errors")
