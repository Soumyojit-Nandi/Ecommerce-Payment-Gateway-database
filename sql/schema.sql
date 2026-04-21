-- E-Commerce Payment Gateway Database Schema
-- Author: [Your Name]
-- Date: April 2026

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS refunds CASCADE;
DROP TABLE IF EXISTS fraud_alerts CASCADE;
DROP TABLE IF EXISTS transaction_logs CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS payment_methods CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;
DROP TABLE IF EXISTS payment_gateways CASCADE;

-- Payment Gateway Providers Table
CREATE TABLE payment_gateways (
    gateway_id SERIAL PRIMARY KEY,
    gateway_name VARCHAR(100) NOT NULL,
    gateway_type VARCHAR(50) NOT NULL,
    api_version VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Merchants Table
CREATE TABLE merchants (
    merchant_id SERIAL PRIMARY KEY,
    merchant_name VARCHAR(200) NOT NULL,
    merchant_email VARCHAR(100) UNIQUE NOT NULL,
    business_type VARCHAR(100),
    country VARCHAR(50),
    currency VARCHAR(10) DEFAULT 'USD',
    commission_rate DECIMAL(5,2) DEFAULT 2.5,
    is_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customers Table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_email VARCHAR(100) UNIQUE NOT NULL,
    customer_name VARCHAR(200),
    phone_number VARCHAR(20),
    country VARCHAR(50),
    customer_since DATE DEFAULT CURRENT_DATE,
    total_transactions INT DEFAULT 0,
    total_spent DECIMAL(15,2) DEFAULT 0.00,
    risk_score INT DEFAULT 50,
    is_blocked BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Payment Methods Table
CREATE TABLE payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    payment_type VARCHAR(50) NOT NULL, -- 'credit_card', 'debit_card', 'wallet', 'upi', 'net_banking'
    card_last_4 VARCHAR(4),
    card_brand VARCHAR(50), -- 'Visa', 'Mastercard', 'Amex', etc.
    expiry_month INT,
    expiry_year INT,
    is_default BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions Table (Main table)
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_uuid UUID UNIQUE NOT NULL,
    merchant_id INT REFERENCES merchants(merchant_id),
    customer_id INT REFERENCES customers(customer_id),
    payment_method_id INT REFERENCES payment_methods(payment_method_id),
    gateway_id INT REFERENCES payment_gateways(gateway_id),
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'USD',
    transaction_status VARCHAR(50) NOT NULL, -- 'pending', 'success', 'failed', 'refunded'
    payment_type VARCHAR(50),
    description TEXT,
    customer_ip VARCHAR(45),
    user_agent TEXT,
    transaction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_timestamp TIMESTAMP,
    gateway_response_code VARCHAR(20),
    gateway_response_message TEXT,
    is_test_transaction BOOLEAN DEFAULT FALSE,
    metadata JSONB
);

-- Transaction Logs Table (Audit trail)
CREATE TABLE transaction_logs (
    log_id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(transaction_id),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_status VARCHAR(50),
    new_status VARCHAR(50),
    changed_by VARCHAR(100),
    remarks TEXT
);

-- Fraud Detection Alerts Table
CREATE TABLE fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(transaction_id),
    alert_type VARCHAR(100) NOT NULL,
    risk_level VARCHAR(20), -- 'low', 'medium', 'high', 'critical'
    alert_message TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

-- Refunds Table
CREATE TABLE refunds (
    refund_id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(transaction_id),
    refund_amount DECIMAL(15,2) NOT NULL,
    refund_reason VARCHAR(200),
    refund_status VARCHAR(50), -- 'pending', 'processed', 'rejected'
    requested_by VARCHAR(100),
    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

-- Indexes for Performance Optimization
CREATE INDEX idx_transactions_merchant ON transactions(merchant_id);
CREATE INDEX idx_transactions_customer ON transactions(customer_id);
CREATE INDEX idx_transactions_status ON transactions(transaction_status);
CREATE INDEX idx_transactions_timestamp ON transactions(transaction_timestamp);
CREATE INDEX idx_transactions_uuid ON transactions(transaction_uuid);
CREATE INDEX idx_fraud_alerts_transaction ON fraud_alerts(transaction_id);
CREATE INDEX idx_refunds_transaction ON refunds(transaction_id);

-- Create partitions for transactions table (by month)
-- This is for large-scale data handling
CREATE TABLE transactions_2026_04 PARTITION OF transactions
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE transactions_2026_05 PARTITION OF transactions
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

-- Insert Sample Payment Gateways
INSERT INTO payment_gateways (gateway_name, gateway_type, api_version) VALUES
('Stripe', 'Card Processing', 'v1'),
('PayPal', 'Digital Wallet', 'v2'),
('Razorpay', 'Unified Payment', 'v1'),
('Square', 'Card Processing', 'v2');

-- Views for Analytics
CREATE OR REPLACE VIEW daily_transaction_summary AS
SELECT 
    DATE(transaction_timestamp) as transaction_date,
    merchant_id,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN transaction_status = 'success' THEN 1 ELSE 0 END) as successful_transactions,
    SUM(CASE WHEN transaction_status = 'failed' THEN 1 ELSE 0 END) as failed_transactions,
    SUM(CASE WHEN transaction_status = 'success' THEN amount ELSE 0 END) as total_revenue,
    AVG(CASE WHEN transaction_status = 'success' THEN amount END) as avg_transaction_value
FROM transactions
GROUP BY DATE(transaction_timestamp), merchant_id;

CREATE OR REPLACE VIEW high_risk_customers AS
SELECT 
    c.customer_id,
    c.customer_email,
    c.customer_name,
    c.risk_score,
    COUNT(DISTINCT t.transaction_id) as total_transactions,
    COUNT(DISTINCT fa.alert_id) as fraud_alerts_count,
    SUM(CASE WHEN t.transaction_status = 'failed' THEN 1 ELSE 0 END) as failed_transactions
FROM customers c
LEFT JOIN transactions t ON c.customer_id = t.customer_id
LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
WHERE c.risk_score > 70 OR c.is_blocked = TRUE
GROUP BY c.customer_id, c.customer_email, c.customer_name, c.risk_score;
