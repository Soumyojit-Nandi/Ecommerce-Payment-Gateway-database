"""
E-Commerce Payment Gateway Data Generator
Generates realistic payment transaction data for testing and analytics
"""

import pandas as pd
import numpy as np
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import json

fake = Faker()
Faker.seed(42)
np.random.seed(42)


class PaymentDataGenerator:
    """Generate realistic payment gateway transaction data"""
    
    def __init__(self, num_merchants=50, num_customers=1000, num_transactions=10000):
        self.num_merchants = num_merchants
        self.num_customers = num_customers
        self.num_transactions = num_transactions
        
        self.payment_types = ['credit_card', 'debit_card', 'wallet', 'upi', 'net_banking']
        self.card_brands = ['Visa', 'Mastercard', 'Amex', 'Discover', 'RuPay']
        self.currencies = ['USD', 'EUR', 'GBP', 'INR', 'CAD']
        self.transaction_statuses = ['success', 'failed', 'pending', 'refunded']
        self.countries = ['USA', 'India', 'UK', 'Canada', 'Australia', 'Germany', 'France']
        self.business_types = ['Electronics', 'Fashion', 'Food', 'Services', 'Books', 'Entertainment']
        
    def generate_merchants(self):
        """Generate merchant data"""
        merchants = []
        for i in range(self.num_merchants):
            merchants.append({
                'merchant_id': i + 1,
                'merchant_name': fake.company(),
                'merchant_email': fake.company_email(),
                'business_type': random.choice(self.business_types),
                'country': random.choice(self.countries),
                'currency': random.choice(self.currencies),
                'commission_rate': round(random.uniform(1.5, 5.0), 2),
                'is_verified': random.choice([True, True, True, False]),  # 75% verified
                'created_at': fake.date_time_between(start_date='-2y', end_date='now')
            })
        return pd.DataFrame(merchants)
    
    def generate_customers(self):
        """Generate customer data"""
        customers = []
        for i in range(self.num_customers):
            risk_score = np.random.normal(50, 20)  # Normal distribution around 50
            risk_score = max(0, min(100, int(risk_score)))  # Clamp between 0-100
            
            customers.append({
                'customer_id': i + 1,
                'customer_email': fake.email(),
                'customer_name': fake.name(),
                'phone_number': fake.phone_number(),
                'country': random.choice(self.countries),
                'customer_since': fake.date_between(start_date='-3y', end_date='today'),
                'total_transactions': 0,  # Will be updated later
                'total_spent': 0.0,  # Will be updated later
                'risk_score': risk_score,
                'is_blocked': risk_score > 85,
                'created_at': fake.date_time_between(start_date='-3y', end_date='now')
            })
        return pd.DataFrame(customers)
    
    def generate_payment_methods(self, customers_df):
        """Generate payment methods for customers"""
        payment_methods = []
        method_id = 1
        
        for customer_id in customers_df['customer_id']:
            # Each customer has 1-3 payment methods
            num_methods = random.randint(1, 3)
            
            for i in range(num_methods):
                payment_type = random.choice(self.payment_types)
                
                method = {
                    'payment_method_id': method_id,
                    'customer_id': customer_id,
                    'payment_type': payment_type,
                    'is_default': i == 0,
                    'is_active': random.choice([True, True, True, False]),
                    'created_at': fake.date_time_between(start_date='-2y', end_date='now')
                }
                
                if payment_type in ['credit_card', 'debit_card']:
                    method['card_last_4'] = str(random.randint(1000, 9999))
                    method['card_brand'] = random.choice(self.card_brands)
                    method['expiry_month'] = random.randint(1, 12)
                    method['expiry_year'] = random.randint(2026, 2030)
                else:
                    method['card_last_4'] = None
                    method['card_brand'] = None
                    method['expiry_month'] = None
                    method['expiry_year'] = None
                
                payment_methods.append(method)
                method_id += 1
        
        return pd.DataFrame(payment_methods)
    
    def generate_transactions(self, merchants_df, customers_df, payment_methods_df):
        """Generate realistic transaction data"""
        transactions = []
        
        # Get active payment methods
        active_methods = payment_methods_df[payment_methods_df['is_active'] == True]
        
        for i in range(self.num_transactions):
            # Random selections
            merchant = merchants_df.sample(1).iloc[0]
            customer = customers_df.sample(1).iloc[0]
            
            # Get payment method for this customer
            customer_methods = active_methods[active_methods['customer_id'] == customer['customer_id']]
            if len(customer_methods) == 0:
                continue
            
            payment_method = customer_methods.sample(1).iloc[0]
            
            # Transaction amount - realistic distribution
            if merchant['business_type'] in ['Electronics', 'Entertainment']:
                amount = round(random.uniform(50, 2000), 2)
            elif merchant['business_type'] in ['Fashion', 'Books']:
                amount = round(random.uniform(20, 500), 2)
            else:
                amount = round(random.uniform(10, 200), 2)
            
            # Status distribution: 85% success, 10% failed, 3% pending, 2% refunded
            status = random.choices(
                self.transaction_statuses,
                weights=[85, 10, 3, 2],
                k=1
            )[0]
            
            # High-risk customers have higher failure rates
            if customer['risk_score'] > 70:
                status = random.choices(
                    ['success', 'failed', 'pending'],
                    weights=[60, 30, 10],
                    k=1
                )[0]
            
            # Transaction timestamp - last 90 days
            transaction_time = fake.date_time_between(start_date='-90d', end_date='now')
            
            # Gateway response codes
            if status == 'success':
                response_code = '00'
                response_message = 'Transaction approved'
            elif status == 'failed':
                response_code = random.choice(['05', '51', '54', '61', '65'])
                response_message = random.choice([
                    'Insufficient funds',
                    'Card declined',
                    'Invalid card number',
                    'Expired card',
                    'Transaction limit exceeded'
                ])
            elif status == 'pending':
                response_code = 'P1'
                response_message = 'Transaction pending verification'
            else:  # refunded
                response_code = 'R0'
                response_message = 'Transaction refunded'
            
            transaction = {
                'transaction_id': i + 1,
                'transaction_uuid': str(uuid.uuid4()),
                'merchant_id': merchant['merchant_id'],
                'customer_id': customer['customer_id'],
                'payment_method_id': payment_method['payment_method_id'],
                'gateway_id': random.randint(1, 4),
                'amount': amount,
                'currency': merchant['currency'],
                'transaction_status': status,
                'payment_type': payment_method['payment_type'],
                'description': f"Payment to {merchant['merchant_name']}",
                'customer_ip': fake.ipv4(),
                'user_agent': fake.user_agent(),
                'transaction_timestamp': transaction_time,
                'processed_timestamp': transaction_time + timedelta(seconds=random.randint(1, 30)) if status != 'pending' else None,
                'gateway_response_code': response_code,
                'gateway_response_message': response_message,
                'is_test_transaction': False,
                'metadata': json.dumps({
                    'device_type': random.choice(['mobile', 'desktop', 'tablet']),
                    'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                    'merchant_reference': f"ORD-{random.randint(10000, 99999)}"
                })
            }
            
            transactions.append(transaction)
        
        return pd.DataFrame(transactions)
    
    def generate_fraud_alerts(self, transactions_df):
        """Generate fraud alerts for suspicious transactions"""
        fraud_alerts = []
        alert_id = 1
        
        # Generate fraud alerts for ~5% of transactions
        suspicious_txns = transactions_df.sample(frac=0.05)
        
        alert_types = [
            'Unusual spending pattern',
            'Multiple failed attempts',
            'Velocity check failed',
            'Geographic anomaly',
            'Card testing detected',
            'High-risk IP address'
        ]
        
        for _, txn in suspicious_txns.iterrows():
            risk_level = random.choices(
                ['low', 'medium', 'high', 'critical'],
                weights=[30, 40, 20, 10],
                k=1
            )[0]
            
            fraud_alerts.append({
                'alert_id': alert_id,
                'transaction_id': txn['transaction_id'],
                'alert_type': random.choice(alert_types),
                'risk_level': risk_level,
                'alert_message': f"Suspicious activity detected: {random.choice(alert_types)}",
                'is_resolved': random.choice([True, False]),
                'resolved_by': fake.name() if random.random() > 0.3 else None,
                'created_at': txn['transaction_timestamp'],
                'resolved_at': txn['transaction_timestamp'] + timedelta(hours=random.randint(1, 48)) if random.random() > 0.3 else None
            })
            alert_id += 1
        
        return pd.DataFrame(fraud_alerts)
    
    def generate_refunds(self, transactions_df):
        """Generate refund records"""
        refunds = []
        refund_id = 1
        
        # Get refunded transactions
        refunded_txns = transactions_df[transactions_df['transaction_status'] == 'refunded']
        
        refund_reasons = [
            'Customer request',
            'Product not delivered',
            'Defective product',
            'Duplicate transaction',
            'Fraudulent transaction'
        ]
        
        for _, txn in refunded_txns.iterrows():
            refunds.append({
                'refund_id': refund_id,
                'transaction_id': txn['transaction_id'],
                'refund_amount': txn['amount'],
                'refund_reason': random.choice(refund_reasons),
                'refund_status': random.choice(['processed', 'pending', 'rejected']),
                'requested_by': fake.name(),
                'requested_at': txn['transaction_timestamp'] + timedelta(days=random.randint(1, 30)),
                'processed_at': txn['transaction_timestamp'] + timedelta(days=random.randint(1, 45))
            })
            refund_id += 1
        
        return pd.DataFrame(refunds)
    
    def generate_all_data(self, output_dir='data/raw'):
        """Generate all datasets and save to CSV"""
        print("Generating merchants data...")
        merchants_df = self.generate_merchants()
        merchants_df.to_csv(f'{output_dir}/merchants.csv', index=False)
        print(f"Generated {len(merchants_df)} merchants")
        
        print("Generating customers data...")
        customers_df = self.generate_customers()
        customers_df.to_csv(f'{output_dir}/customers.csv', index=False)
        print(f"Generated {len(customers_df)} customers")
        
        print("Generating payment methods data...")
        payment_methods_df = self.generate_payment_methods(customers_df)
        payment_methods_df.to_csv(f'{output_dir}/payment_methods.csv', index=False)
        print(f"Generated {len(payment_methods_df)} payment methods")
        
        print("Generating transactions data...")
        transactions_df = self.generate_transactions(merchants_df, customers_df, payment_methods_df)
        transactions_df.to_csv(f'{output_dir}/transactions.csv', index=False)
        print(f"Generated {len(transactions_df)} transactions")
        
        print("Generating fraud alerts data...")
        fraud_alerts_df = self.generate_fraud_alerts(transactions_df)
        fraud_alerts_df.to_csv(f'{output_dir}/fraud_alerts.csv', index=False)
        print(f"Generated {len(fraud_alerts_df)} fraud alerts")
        
        print("Generating refunds data...")
        refunds_df = self.generate_refunds(transactions_df)
        refunds_df.to_csv(f'{output_dir}/refunds.csv', index=False)
        print(f"Generated {len(refunds_df)} refunds")
        
        print("\nData generation complete!")
        print(f"Files saved to: {output_dir}/")
        
        return {
            'merchants': merchants_df,
            'customers': customers_df,
            'payment_methods': payment_methods_df,
            'transactions': transactions_df,
            'fraud_alerts': fraud_alerts_df,
            'refunds': refunds_df
        }


if __name__ == "__main__":
    generator = PaymentDataGenerator(
        num_merchants=50,
        num_customers=1000,
        num_transactions=10000
    )
    
    data = generator.generate_all_data()
    
    # Print summary statistics
    print("\n=== Data Summary ===")
    print(f"Total Merchants: {len(data['merchants'])}")
    print(f"Total Customers: {len(data['customers'])}")
    print(f"Total Transactions: {len(data['transactions'])}")
    print(f"Total Revenue: ${data['transactions']['amount'].sum():,.2f}")
    print(f"Average Transaction: ${data['transactions']['amount'].mean():.2f}")
    print(f"\nTransaction Status Distribution:")
    print(data['transactions']['transaction_status'].value_counts())
