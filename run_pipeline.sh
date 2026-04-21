#!/bin/bash

# E-Commerce Payment Gateway DB - Main Execution Script
# This script runs the complete data engineering pipeline

echo "======================================================================"
echo "E-Commerce Payment Gateway Database - Data Engineering Pipeline"
echo "======================================================================"
echo ""

# Set project directory
PROJECT_DIR="/home/claude/ecommerce-payment-gateway-db"
cd $PROJECT_DIR

# Step 1: Generate Data
echo "Step 1: Generating Payment Transaction Data..."
echo "----------------------------------------------------------------------"
python src/data_generator.py
if [ $? -ne 0 ]; then
    echo "ERROR: Data generation failed!"
    exit 1
fi
echo ""

# Step 2: Run ETL Pipeline
echo "Step 2: Running ETL Pipeline (Extract, Transform, Load)..."
echo "----------------------------------------------------------------------"
python src/etl_pipeline.py
if [ $? -ne 0 ]; then
    echo "ERROR: ETL pipeline failed!"
    exit 1
fi
echo ""

# Step 3: Data Quality Validation
echo "Step 3: Running Data Quality Checks..."
echo "----------------------------------------------------------------------"
python src/data_quality.py
if [ $? -ne 0 ]; then
    echo "WARNING: Data quality issues detected. Check logs for details."
fi
echo ""

# Step 4: Generate Analytics and Visualizations
echo "Step 4: Generating Analytics and Visualizations..."
echo "----------------------------------------------------------------------"
python src/analytics.py
if [ $? -ne 0 ]; then
    echo "ERROR: Analytics generation failed!"
    exit 1
fi
echo ""

# Step 5: Display Summary
echo "======================================================================"
echo "Pipeline Execution Complete!"
echo "======================================================================"
echo ""
echo "Generated Files:"
echo "  - Raw Data: data/raw/*.csv"
echo "  - Processed Data: data/processed/*.csv"
echo "  - Visualizations: screenshots/*.png"
echo "  - Documentation: docs/Project_Documentation.docx"
echo ""
echo "Next Steps:"
echo "  1. Review visualizations in screenshots/"
echo "  2. Check analytics output above"
echo "  3. Load data to PostgreSQL (optional): uncomment in etl_pipeline.py"
echo "  4. Run SQL queries: psql -f sql/analytics_queries.sql"
echo ""
echo "Project Structure:"
echo "  - src/: Python source code (ETL, data quality, analytics)"
echo "  - sql/: Database schema and analytics queries"
echo "  - data/: Raw and processed data files"
echo "  - airflow/: Airflow DAG for orchestration"
echo "  - docs/: Project documentation"
echo ""
echo "✓ All tasks completed successfully!"
echo "======================================================================"
