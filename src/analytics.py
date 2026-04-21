"""
Analytics and Visualization for Payment Gateway Data
Generates insights and charts from processed data
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set style
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = (12, 6)


class PaymentAnalytics:
    """Payment Gateway Analytics and Reporting"""
    
    def __init__(self, data_dir='data/raw'):
        """Load data from CSV files"""
        print("Loading data...")
        self.merchants = pd.read_csv(f'{data_dir}/merchants.csv')
        self.customers = pd.read_csv(f'{data_dir}/customers.csv')
        self.payment_methods = pd.read_csv(f'{data_dir}/payment_methods.csv')
        self.transactions = pd.read_csv(f'{data_dir}/transactions.csv')
        self.fraud_alerts = pd.read_csv(f'{data_dir}/fraud_alerts.csv')
        self.refunds = pd.read_csv(f'{data_dir}/refunds.csv')
        
        # Convert dates
        self.transactions['transaction_timestamp'] = pd.to_datetime(
            self.transactions['transaction_timestamp']
        )
        print(f"Data loaded successfully: {len(self.transactions)} transactions")
    
    def generate_summary_report(self):
        """Generate executive summary report"""
        print("\n" + "=" * 70)
        print("PAYMENT GATEWAY ANALYTICS - EXECUTIVE SUMMARY")
        print("=" * 70)
        
        # Key Metrics
        total_txns = len(self.transactions)
        successful_txns = len(self.transactions[self.transactions['transaction_status'] == 'success'])
        total_revenue = self.transactions[self.transactions['transaction_status'] == 'success']['amount'].sum()
        avg_txn_value = self.transactions[self.transactions['transaction_status'] == 'success']['amount'].mean()
        
        print(f"\n📊 KEY METRICS")
        print(f"  Total Transactions: {total_txns:,}")
        print(f"  Successful Transactions: {successful_txns:,}")
        print(f"  Total Revenue: ${total_revenue:,.2f}")
        print(f"  Average Transaction Value: ${avg_txn_value:,.2f}")
        print(f"  Success Rate: {(successful_txns/total_txns*100):.2f}%")
        
        # Transaction Status Distribution
        print(f"\n📈 TRANSACTION STATUS")
        status_dist = self.transactions['transaction_status'].value_counts()
        for status, count in status_dist.items():
            pct = (count/total_txns) * 100
            print(f"  {status.capitalize()}: {count:,} ({pct:.2f}%)")
        
        # Top Merchants
        print(f"\n🏆 TOP 5 MERCHANTS BY REVENUE")
        merchant_revenue = self.transactions[
            self.transactions['transaction_status'] == 'success'
        ].groupby('merchant_id')['amount'].sum().sort_values(ascending=False).head(5)
        
        for idx, (merchant_id, revenue) in enumerate(merchant_revenue.items(), 1):
            merchant_name = self.merchants[self.merchants['merchant_id'] == merchant_id]['merchant_name'].values[0]
            print(f"  {idx}. {merchant_name}: ${revenue:,.2f}")
        
        # Customer Insights
        print(f"\n👥 CUSTOMER INSIGHTS")
        total_customers = len(self.customers)
        active_customers = self.transactions['customer_id'].nunique()
        high_risk = len(self.customers[self.customers['risk_score'] > 70])
        
        print(f"  Total Customers: {total_customers:,}")
        print(f"  Active Customers: {active_customers:,}")
        print(f"  High-Risk Customers: {high_risk:,} ({(high_risk/total_customers*100):.2f}%)")
        
        # Fraud Alerts
        print(f"\n🚨 FRAUD DETECTION")
        print(f"  Total Fraud Alerts: {len(self.fraud_alerts):,}")
        print(f"  Resolved Alerts: {len(self.fraud_alerts[self.fraud_alerts['is_resolved'] == True]):,}")
        print(f"  Pending Alerts: {len(self.fraud_alerts[self.fraud_alerts['is_resolved'] == False]):,}")
        
        risk_dist = self.fraud_alerts['risk_level'].value_counts()
        print(f"\n  Risk Level Distribution:")
        for level, count in risk_dist.items():
            print(f"    {level.capitalize()}: {count:,}")
        
        # Payment Methods
        print(f"\n💳 PAYMENT METHODS")
        payment_dist = self.transactions['payment_type'].value_counts()
        for method, count in payment_dist.items():
            pct = (count/total_txns) * 100
            print(f"  {method.replace('_', ' ').title()}: {count:,} ({pct:.2f}%)")
        
        print("\n" + "=" * 70)
    
    def create_visualizations(self, output_dir='screenshots'):
        """Create visualization charts"""
        print("\nGenerating visualizations...")
        
        # 1. Transaction Status Distribution
        plt.figure(figsize=(10, 6))
        status_counts = self.transactions['transaction_status'].value_counts()
        colors = ['#2ecc71', '#e74c3c', '#f39c12', '#95a5a6']
        plt.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%', 
                colors=colors, startangle=90)
        plt.title('Transaction Status Distribution', fontsize=16, fontweight='bold')
        plt.savefig(f'{output_dir}/01_transaction_status.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ Transaction status chart created")
        
        # 2. Daily Transaction Volume
        plt.figure(figsize=(14, 6))
        daily_txns = self.transactions.groupby(
            self.transactions['transaction_timestamp'].dt.date
        ).size()
        plt.plot(daily_txns.index, daily_txns.values, marker='o', linewidth=2, color='#3498db')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Number of Transactions', fontsize=12)
        plt.title('Daily Transaction Volume', fontsize=16, fontweight='bold')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/02_daily_volume.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ Daily volume chart created")
        
        # 3. Revenue by Payment Method
        plt.figure(figsize=(12, 6))
        payment_revenue = self.transactions[
            self.transactions['transaction_status'] == 'success'
        ].groupby('payment_type')['amount'].sum().sort_values(ascending=False)
        
        plt.bar(range(len(payment_revenue)), payment_revenue.values, color='#9b59b6')
        plt.xlabel('Payment Method', fontsize=12)
        plt.ylabel('Total Revenue ($)', fontsize=12)
        plt.title('Revenue by Payment Method', fontsize=16, fontweight='bold')
        plt.xticks(range(len(payment_revenue)), 
                   [x.replace('_', ' ').title() for x in payment_revenue.index],
                   rotation=45)
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/03_payment_method_revenue.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ Payment method revenue chart created")
        
        # 4. Hourly Transaction Pattern
        plt.figure(figsize=(14, 6))
        self.transactions['hour'] = pd.to_datetime(
            self.transactions['transaction_timestamp']
        ).dt.hour
        hourly = self.transactions.groupby('hour').size()
        
        plt.bar(hourly.index, hourly.values, color='#e67e22', alpha=0.7)
        plt.xlabel('Hour of Day', fontsize=12)
        plt.ylabel('Number of Transactions', fontsize=12)
        plt.title('Transaction Volume by Hour of Day', fontsize=16, fontweight='bold')
        plt.xticks(range(24))
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/04_hourly_pattern.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ Hourly pattern chart created")
        
        # 5. Customer Risk Score Distribution
        plt.figure(figsize=(12, 6))
        plt.hist(self.customers['risk_score'], bins=20, color='#e74c3c', alpha=0.7, edgecolor='black')
        plt.axvline(70, color='red', linestyle='--', linewidth=2, label='High Risk Threshold')
        plt.xlabel('Risk Score', fontsize=12)
        plt.ylabel('Number of Customers', fontsize=12)
        plt.title('Customer Risk Score Distribution', fontsize=16, fontweight='bold')
        plt.legend()
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/05_risk_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ Risk distribution chart created")
        
        # 6. Top 10 Merchants
        plt.figure(figsize=(14, 8))
        top_merchants = self.transactions[
            self.transactions['transaction_status'] == 'success'
        ].groupby('merchant_id')['amount'].sum().sort_values(ascending=False).head(10)
        
        merchant_names = []
        for mid in top_merchants.index:
            name = self.merchants[self.merchants['merchant_id'] == mid]['merchant_name'].values[0]
            merchant_names.append(name[:30] + '...' if len(name) > 30 else name)
        
        plt.barh(range(len(top_merchants)), top_merchants.values, color='#1abc9c')
        plt.yticks(range(len(top_merchants)), merchant_names)
        plt.xlabel('Total Revenue ($)', fontsize=12)
        plt.title('Top 10 Merchants by Revenue', fontsize=16, fontweight='bold')
        plt.grid(True, alpha=0.3, axis='x')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/06_top_merchants.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ Top merchants chart created")
        
        print(f"\nAll visualizations saved to: {output_dir}/")
    
    def customer_segmentation_analysis(self):
        """Analyze and segment customers"""
        print("\n" + "=" * 70)
        print("CUSTOMER SEGMENTATION ANALYSIS")
        print("=" * 70)
        
        # Calculate customer lifetime value
        customer_txns = self.transactions[
            self.transactions['transaction_status'] == 'success'
        ].groupby('customer_id').agg({
            'amount': ['sum', 'mean', 'count']
        }).round(2)
        
        customer_txns.columns = ['total_spent', 'avg_transaction', 'transaction_count']
        customer_txns = customer_txns.reset_index()
        
        # Segment customers
        def segment_customer(row):
            if row['total_spent'] > 5000:
                return 'VIP'
            elif row['total_spent'] > 2000:
                return 'Premium'
            elif row['total_spent'] > 500:
                return 'Regular'
            else:
                return 'New'
        
        customer_txns['segment'] = customer_txns.apply(segment_customer, axis=1)
        
        print("\nCustomer Segments:")
        segment_summary = customer_txns.groupby('segment').agg({
            'customer_id': 'count',
            'total_spent': 'sum',
            'avg_transaction': 'mean'
        }).round(2)
        
        for segment in ['VIP', 'Premium', 'Regular', 'New']:
            if segment in segment_summary.index:
                count = segment_summary.loc[segment, 'customer_id']
                revenue = segment_summary.loc[segment, 'total_spent']
                avg = segment_summary.loc[segment, 'avg_transaction']
                print(f"\n  {segment}:")
                print(f"    Customers: {count:,}")
                print(f"    Total Revenue: ${revenue:,.2f}")
                print(f"    Avg Transaction: ${avg:.2f}")
        
        print("\n" + "=" * 70)


if __name__ == "__main__":
    # Create analytics instance
    analytics = PaymentAnalytics()
    
    # Generate summary report
    analytics.generate_summary_report()
    
    # Create visualizations
    analytics.create_visualizations()
    
    # Customer segmentation
    analytics.customer_segmentation_analysis()
    
    print("\n✅ Analytics completed successfully!")
