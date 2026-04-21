# E-Commerce Payment Gateway - Integrated System Setup
## Apache Airflow + Kafka + Spark + PostgreSQL

---

## 🚀 Quick Start with Docker

### Prerequisites
- Docker Desktop installed
- 8GB RAM minimum
- 20GB free disk space

### Step 1: Start All Services

```bash
cd ecommerce-payment-gateway-db

# Start all services (Postgres, Kafka, Spark, Airflow)
docker-compose up -d

# Check services are running
docker-compose ps
```

**Services will be available at:**
- PostgreSQL: `localhost:5432`
- Kafka: `localhost:9092`
- Spark Master UI: `http://localhost:8080`
- Airflow Web UI: `http://localhost:8081`

### Step 2: Initialize Airflow

```bash
# Initialize Airflow database
docker exec -it airflow-webserver airflow db init

# Create admin user
docker exec -it airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### Step 3: Access Airflow UI

1. Open browser: `http://localhost:8081`
2. Login: `admin` / `admin`
3. Enable DAG: `payment_gateway_advanced_pipeline`
4. Trigger the DAG manually

---

## 📊 Architecture Overview

```
┌─────────────────┐
│  Data Sources   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│   Apache Kafka          │
│   (Real-time Stream)    │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   Kafka Consumer        │
│   (Process & Store)     │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   PostgreSQL            │
│   (8 Normalized Tables) │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   Apache Spark          │
│   (Batch Analytics)     │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   Apache Airflow        │
│   (Orchestration)       │
└─────────────────────────┘
```

---

## 🔄 Running Individual Components

### 1. Kafka Producer (Real-time Streaming)

```bash
# Install dependencies
pip install kafka-python faker

# Run producer to stream transactions
python src/kafka_producer.py
```

**Output:**
```
INFO:__main__:Produced transaction 1/100: xxx-xxx-xxx - success
INFO:__main__:Produced transaction 2/100: xxx-xxx-xxx - failed
...
```

### 2. Kafka Consumer (Process Stream)

```bash
# In a new terminal, run consumer
python src/kafka_consumer.py
```

**Output:**
```
INFO:__main__:Starting transaction consumer...
INFO:__main__:Received transaction: xxx-xxx-xxx
INFO:__main__:Processed transaction: xxx-xxx-xxx
```

### 3. Spark Analytics (Batch Processing)

```bash
# Install PySpark
pip install pyspark

# Run Spark analytics
python src/spark_analytics.py
```

**Output:**
```
Starting Full Spark Analytics Pipeline
Analyzing transaction success rates...
Detecting fraud patterns...
Calculating customer lifetime value...
```

### 4. Airflow Pipeline (Orchestration)

Access Airflow UI and trigger the DAG, or use CLI:

```bash
# Trigger DAG via CLI
docker exec -it airflow-scheduler airflow dags trigger payment_gateway_advanced_pipeline
```

---

## 🛠️ Manual Setup (Without Docker)

### 1. Install PostgreSQL

```bash
# Install PostgreSQL
sudo apt-get install postgresql postgresql-contrib

# Create database
sudo -u postgres psql
CREATE DATABASE payment_gateway_db;
\q

# Run schema
psql -U postgres -d payment_gateway_db -f sql/schema.sql
```

### 2. Install Kafka

```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
cd kafka_2.13-3.5.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Start Kafka
bin/kafka-server-start.sh config/server.properties &

# Create topic
bin/kafka-topics.sh --create --topic payment-transactions \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 3
```

### 3. Install Spark

```bash
# Download Spark
wget https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar -xzf spark-3.4.1-bin-hadoop3.tgz
export SPARK_HOME=~/spark-3.4.1-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

### 4. Install Airflow

```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Install Airflow
pip install apache-airflow==2.7.0

# Initialize database
airflow db init

# Create user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start webserver and scheduler
airflow webserver --port 8080 &
airflow scheduler &
```

---

## 📈 Data Flow Examples

### Example 1: Real-time Transaction Processing

```bash
# Terminal 1: Start Kafka producer
python src/kafka_producer.py

# Terminal 2: Start Kafka consumer  
python src/kafka_consumer.py

# Terminal 3: Monitor PostgreSQL
psql -U postgres -d payment_gateway_db
SELECT COUNT(*) FROM transactions;
```

### Example 2: Batch Analytics with Spark

```bash
# Run Spark job on accumulated data
python src/spark_analytics.py

# View results
# Success rates, fraud patterns, CLV calculated
```

### Example 3: Automated Pipeline with Airflow

```bash
# Access Airflow UI: http://localhost:8081
# Trigger DAG: payment_gateway_advanced_pipeline
# Monitor execution in Graph view
# Check logs for each task
```

---

## 🧪 Testing the System

### Test 1: Kafka Streaming

```bash
# List Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic payment-transactions \
    --from-beginning
```

### Test 2: PostgreSQL Data

```bash
# Check data in PostgreSQL
docker exec -it payment_gateway_postgres psql -U postgres -d payment_gateway_db

# Run query
SELECT 
    transaction_status,
    COUNT(*) as count,
    SUM(amount) as total_amount
FROM transactions
GROUP BY transaction_status;
```

### Test 3: Spark Processing

```bash
# Access Spark Master UI
# http://localhost:8080

# Check running applications
# View completed jobs and metrics
```

---

## 📊 Key Metrics to Monitor

### Kafka Metrics
- Messages per second
- Consumer lag
- Topic partition distribution

### Spark Metrics  
- Job execution time
- Number of tasks
- Shuffle read/write

### PostgreSQL Metrics
- Transaction throughput
- Query performance
- Table sizes

### Airflow Metrics
- DAG run duration
- Task success rate
- Scheduler performance

---

## 🔧 Troubleshooting

### Issue: Kafka connection refused

```bash
# Check Kafka is running
docker logs kafka

# Restart Kafka
docker-compose restart kafka
```

### Issue: Airflow tasks failing

```bash
# Check task logs
docker exec -it airflow-scheduler airflow tasks test \
    payment_gateway_advanced_pipeline generate_batch_data 2024-01-01

# View container logs
docker logs airflow-scheduler
```

### Issue: Spark job errors

```bash
# Check Spark logs
docker logs spark-master
docker logs spark-worker

# Verify Spark is accessible
curl http://localhost:8080
```

---

## 🎯 Production Considerations

1. **Kafka**: Configure replication factor > 1 for fault tolerance
2. **Spark**: Increase worker nodes for horizontal scaling  
3. **PostgreSQL**: Set up read replicas for analytics queries
4. **Airflow**: Use CeleryExecutor for distributed task execution
5. **Monitoring**: Add Prometheus + Grafana for metrics
6. **Security**: Enable SSL/TLS, authentication, and encryption

---

## 📚 Additional Resources

- Kafka Documentation: https://kafka.apache.org/documentation/
- Spark Guide: https://spark.apache.org/docs/latest/
- Airflow Docs: https://airflow.apache.org/docs/
- PostgreSQL Manual: https://www.postgresql.org/docs/

---

## ✅ Verification Checklist

- [ ] Docker services running
- [ ] Kafka topic created
- [ ] PostgreSQL schema loaded
- [ ] Airflow UI accessible
- [ ] Spark master UI accessible
- [ ] Kafka producer running
- [ ] Transactions flowing to database
- [ ] Spark analytics completing
- [ ] Airflow DAG successful

**System is ready when all checks pass!** ✓
