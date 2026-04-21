-- Advanced Analytics Queries for Payment Gateway Database
-- Author: [Your Name]
-- Date: April 2026

-- =====================================
-- 1. Daily Revenue and Transaction Analysis
-- =====================================
SELECT 
    DATE(transaction_timestamp) as transaction_date,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT merchant_id) as unique_merchants,
    SUM(CASE WHEN transaction_status = 'success' THEN amount ELSE 0 END) as total_revenue,
    SUM(CASE WHEN transaction_status = 'failed' THEN 1 ELSE 0 END) as failed_count,
    SUM(CASE WHEN transaction_status = 'refunded' THEN amount ELSE 0 END) as refunded_amount,
    ROUND(AVG(CASE WHEN transaction_status = 'success' THEN amount END), 2) as avg_transaction_value,
    ROUND(
        100.0 * SUM(CASE WHEN transaction_status = 'success' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as success_rate_pct
FROM transactions
WHERE transaction_timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(transaction_timestamp)
ORDER BY transaction_date DESC;


-- =====================================
-- 2. Top Performing Merchants
-- =====================================
SELECT 
    m.merchant_id,
    m.merchant_name,
    m.business_type,
    COUNT(t.transaction_id) as total_transactions,
    SUM(CASE WHEN t.transaction_status = 'success' THEN t.amount ELSE 0 END) as total_revenue,
    ROUND(AVG(CASE WHEN t.transaction_status = 'success' THEN t.amount END), 2) as avg_order_value,
    ROUND(
        100.0 * SUM(CASE WHEN t.transaction_status = 'success' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as success_rate_pct,
    COUNT(DISTINCT t.customer_id) as unique_customers
FROM merchants m
JOIN transactions t ON m.merchant_id = t.merchant_id
WHERE t.transaction_timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY m.merchant_id, m.merchant_name, m.business_type
HAVING COUNT(t.transaction_id) >= 10
ORDER BY total_revenue DESC
LIMIT 20;


-- =====================================
-- 3. Customer Lifetime Value (CLV)
-- =====================================
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.customer_email,
        c.risk_score,
        COUNT(t.transaction_id) as total_transactions,
        SUM(CASE WHEN t.transaction_status = 'success' THEN t.amount ELSE 0 END) as lifetime_value,
        AVG(CASE WHEN t.transaction_status = 'success' THEN t.amount END) as avg_transaction_value,
        MAX(t.transaction_timestamp) as last_transaction_date,
        MIN(t.transaction_timestamp) as first_transaction_date,
        EXTRACT(DAYS FROM (MAX(t.transaction_timestamp) - MIN(t.transaction_timestamp))) as customer_age_days
    FROM customers c
    LEFT JOIN transactions t ON c.customer_id = t.customer_id
    GROUP BY c.customer_id, c.customer_name, c.customer_email, c.risk_score
)
SELECT 
    customer_id,
    customer_name,
    customer_email,
    total_transactions,
    ROUND(lifetime_value, 2) as lifetime_value,
    ROUND(avg_transaction_value, 2) as avg_transaction_value,
    risk_score,
    CASE 
        WHEN lifetime_value > 5000 THEN 'VIP'
        WHEN lifetime_value > 2000 THEN 'Premium'
        WHEN lifetime_value > 500 THEN 'Regular'
        ELSE 'New'
    END as customer_segment,
    last_transaction_date,
    CASE 
        WHEN EXTRACT(DAYS FROM (CURRENT_DATE - last_transaction_date)) > 90 THEN 'At Risk'
        WHEN EXTRACT(DAYS FROM (CURRENT_DATE - last_transaction_date)) > 30 THEN 'Inactive'
        ELSE 'Active'
    END as activity_status
FROM customer_metrics
WHERE total_transactions > 0
ORDER BY lifetime_value DESC
LIMIT 100;


-- =====================================
-- 4. Payment Method Performance
-- =====================================
SELECT 
    t.payment_type,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN t.transaction_status = 'success' THEN t.amount ELSE 0 END) as total_revenue,
    ROUND(AVG(CASE WHEN t.transaction_status = 'success' THEN t.amount END), 2) as avg_transaction_value,
    ROUND(
        100.0 * SUM(CASE WHEN t.transaction_status = 'success' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as success_rate_pct,
    SUM(CASE WHEN t.transaction_status = 'failed' THEN 1 ELSE 0 END) as failed_count,
    ROUND(
        100.0 * SUM(CASE WHEN t.transaction_status = 'refunded' THEN 1 ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN t.transaction_status = 'success' THEN 1 ELSE 0 END), 0),
        2
    ) as refund_rate_pct
FROM transactions t
WHERE t.transaction_timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY t.payment_type
ORDER BY total_revenue DESC;


-- =====================================
-- 5. Fraud Detection Analysis
-- =====================================
SELECT 
    fa.risk_level,
    fa.alert_type,
    COUNT(*) as alert_count,
    COUNT(DISTINCT fa.transaction_id) as affected_transactions,
    SUM(t.amount) as total_amount_at_risk,
    COUNT(CASE WHEN fa.is_resolved = TRUE THEN 1 END) as resolved_count,
    COUNT(CASE WHEN fa.is_resolved = FALSE THEN 1 END) as pending_count,
    ROUND(AVG(EXTRACT(HOURS FROM (fa.resolved_at - fa.created_at))), 2) as avg_resolution_hours
FROM fraud_alerts fa
JOIN transactions t ON fa.transaction_id = t.transaction_id
WHERE fa.created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY fa.risk_level, fa.alert_type
ORDER BY total_amount_at_risk DESC;


-- =====================================
-- 6. Hourly Transaction Patterns
-- =====================================
SELECT 
    EXTRACT(HOUR FROM transaction_timestamp) as hour_of_day,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN transaction_status = 'success' THEN amount ELSE 0 END) as revenue,
    ROUND(AVG(CASE WHEN transaction_status = 'success' THEN amount END), 2) as avg_value,
    ROUND(
        100.0 * SUM(CASE WHEN transaction_status = 'success' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as success_rate_pct
FROM transactions
WHERE transaction_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY EXTRACT(HOUR FROM transaction_timestamp)
ORDER BY hour_of_day;


-- =====================================
-- 7. Geographic Analysis
-- =====================================
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(t.transaction_id) as transaction_count,
    SUM(CASE WHEN t.transaction_status = 'success' THEN t.amount ELSE 0 END) as total_revenue,
    ROUND(AVG(CASE WHEN t.transaction_status = 'success' THEN t.amount END), 2) as avg_transaction_value,
    ROUND(
        100.0 * SUM(CASE WHEN t.transaction_status = 'success' THEN 1 ELSE 0 END) / COUNT(t.transaction_id),
        2
    ) as success_rate_pct
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
WHERE t.transaction_timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.country
ORDER BY total_revenue DESC;


-- =====================================
-- 8. Refund Analysis
-- =====================================
SELECT 
    r.refund_reason,
    COUNT(*) as refund_count,
    SUM(r.refund_amount) as total_refund_amount,
    ROUND(AVG(r.refund_amount), 2) as avg_refund_amount,
    COUNT(CASE WHEN r.refund_status = 'processed' THEN 1 END) as processed_count,
    COUNT(CASE WHEN r.refund_status = 'pending' THEN 1 END) as pending_count,
    ROUND(AVG(EXTRACT(DAYS FROM (r.processed_at - r.requested_at))), 2) as avg_processing_days
FROM refunds r
WHERE r.requested_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY r.refund_reason
ORDER BY total_refund_amount DESC;


-- =====================================
-- 9. Cohort Analysis - Customer Retention
-- =====================================
WITH first_purchase AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(transaction_timestamp)) as cohort_month
    FROM transactions
    WHERE transaction_status = 'success'
    GROUP BY customer_id
),
customer_activity AS (
    SELECT 
        fp.cohort_month,
        DATE_TRUNC('month', t.transaction_timestamp) as activity_month,
        COUNT(DISTINCT t.customer_id) as active_customers
    FROM first_purchase fp
    JOIN transactions t ON fp.customer_id = t.customer_id
    WHERE t.transaction_status = 'success'
    GROUP BY fp.cohort_month, DATE_TRUNC('month', t.transaction_timestamp)
)
SELECT 
    cohort_month,
    activity_month,
    active_customers,
    EXTRACT(MONTH FROM AGE(activity_month, cohort_month)) as months_since_first_purchase
FROM customer_activity
WHERE cohort_month >= CURRENT_DATE - INTERVAL '6 months'
ORDER BY cohort_month, activity_month;


-- =====================================
-- 10. Gateway Performance Comparison
-- =====================================
SELECT 
    pg.gateway_name,
    COUNT(t.transaction_id) as total_transactions,
    SUM(CASE WHEN t.transaction_status = 'success' THEN t.amount ELSE 0 END) as total_revenue,
    ROUND(
        100.0 * SUM(CASE WHEN t.transaction_status = 'success' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as success_rate_pct,
    ROUND(AVG(EXTRACT(SECONDS FROM (t.processed_timestamp - t.transaction_timestamp))), 2) as avg_processing_seconds,
    COUNT(CASE WHEN t.transaction_status = 'failed' THEN 1 END) as failed_count
FROM payment_gateways pg
JOIN transactions t ON pg.gateway_id = t.gateway_id
WHERE t.transaction_timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY pg.gateway_id, pg.gateway_name
ORDER BY total_revenue DESC;
