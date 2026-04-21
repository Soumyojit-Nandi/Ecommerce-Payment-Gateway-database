"""
PySpark Analytics - Large-scale Transaction Analysis
Processes transactions using Apache Spark for scalable analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, hour, dayofweek
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkTransactionAnalytics:
    """Spark-based analytics for payment transactions"""
    
    def __init__(self, app_name="PaymentGatewayAnalytics"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", "/path/to/postgresql-jdbc.jar") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
    
    def load_transactions_from_postgres(self):
        """Load transactions from PostgreSQL"""
        jdbc_url = "jdbc:postgresql://localhost:5432/payment_gateway_db"
        connection_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table="transactions",
            properties=connection_properties
        )
        
        logger.info(f"Loaded {df.count()} transactions from PostgreSQL")
        return df
    
    def load_transactions_from_csv(self, file_path):
        """Load transactions from CSV file"""
        df = self.spark.read.csv(
            file_path,
            header=True,
            inferSchema=True
        )
        
        logger.info(f"Loaded {df.count()} transactions from CSV")
        return df
    
    def analyze_transaction_success_rate(self, df):
        """Calculate success rate by merchant and payment type"""
        logger.info("Analyzing transaction success rates...")
        
        success_rate = df.groupBy("merchant_id", "payment_type") \
            .agg(
                count("*").alias("total_transactions"),
                sum(when(col("transaction_status") == "success", 1).otherwise(0)).alias("successful"),
                sum(when(col("transaction_status") == "failed", 1).otherwise(0)).alias("failed"),
                avg("amount").alias("avg_transaction_amount")
            ) \
            .withColumn(
                "success_rate",
                (col("successful") / col("total_transactions") * 100)
            ) \
            .orderBy(col("success_rate").desc())
        
        success_rate.show(20)
        return success_rate
    
    def analyze_hourly_patterns(self, df):
        """Analyze transaction patterns by hour"""
        logger.info("Analyzing hourly transaction patterns...")
        
        hourly = df.withColumn("hour", hour(col("transaction_timestamp"))) \
            .groupBy("hour") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_amount")
            ) \
            .orderBy("hour")
        
        hourly.show(24)
        return hourly
    
    def detect_fraud_patterns(self, df):
        """Detect potential fraud using Spark ML patterns"""
        logger.info("Detecting fraud patterns...")
        
        # Define window for customer transactions
        window_spec = Window.partitionBy("customer_id").orderBy("transaction_timestamp")
        
        fraud_indicators = df.withColumn(
            "prev_transaction_time",
            F.lag("transaction_timestamp").over(window_spec)
        ).withColumn(
            "time_diff_seconds",
            F.unix_timestamp("transaction_timestamp") - F.unix_timestamp("prev_transaction_time")
        ).withColumn(
            "is_suspicious",
            when(
                (col("time_diff_seconds") < 60) |  # Multiple transactions within 1 minute
                (col("amount") > 1500) |            # High value transaction
                (hour(col("transaction_timestamp")).isin([0, 1, 2, 3, 4, 5])),  # Late night
                True
            ).otherwise(False)
        )
        
        suspicious_count = fraud_indicators.filter(col("is_suspicious") == True).count()
        logger.info(f"Found {suspicious_count} suspicious transactions")
        
        fraud_indicators.filter(col("is_suspicious") == True).show(10)
        return fraud_indicators
    
    def customer_lifetime_value(self, df):
        """Calculate customer lifetime value (CLV)"""
        logger.info("Calculating customer lifetime value...")
        
        clv = df.filter(col("transaction_status") == "success") \
            .groupBy("customer_id") \
            .agg(
                count("*").alias("total_transactions"),
                sum("amount").alias("lifetime_value"),
                avg("amount").alias("avg_transaction_value"),
                F.max("transaction_timestamp").alias("last_transaction"),
                F.min("transaction_timestamp").alias("first_transaction")
            ) \
            .withColumn(
                "customer_segment",
                when(col("lifetime_value") > 5000, "VIP")
                .when(col("lifetime_value") > 2000, "Premium")
                .when(col("lifetime_value") > 500, "Regular")
                .otherwise("New")
            ) \
            .orderBy(col("lifetime_value").desc())
        
        clv.show(20)
        return clv
    
    def save_results_to_postgres(self, df, table_name):
        """Save analysis results back to PostgreSQL"""
        jdbc_url = "jdbc:postgresql://localhost:5432/payment_gateway_db"
        connection_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
        
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=connection_properties
        )
        
        logger.info(f"Results saved to {table_name}")
    
    def run_full_analysis(self, input_path):
        """Run complete analysis pipeline"""
        logger.info("=" * 60)
        logger.info("Starting Full Spark Analytics Pipeline")
        logger.info("=" * 60)
        
        # Load data
        df = self.load_transactions_from_csv(input_path)
        
        # Run analyses
        success_rate = self.analyze_transaction_success_rate(df)
        hourly_patterns = self.analyze_hourly_patterns(df)
        fraud_patterns = self.detect_fraud_patterns(df)
        clv = self.customer_lifetime_value(df)
        
        logger.info("=" * 60)
        logger.info("Spark Analytics Pipeline Completed")
        logger.info("=" * 60)
        
        return {
            'success_rate': success_rate,
            'hourly_patterns': hourly_patterns,
            'fraud_patterns': fraud_patterns,
            'customer_clv': clv
        }
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    analytics = SparkTransactionAnalytics()
    
    try:
        # Run analytics on CSV data
        results = analytics.run_full_analysis("data/raw/transactions.csv")
        
        # Optionally save results
        # analytics.save_results_to_postgres(results['customer_clv'], 'spark_customer_clv')
        
    finally:
        analytics.stop()
