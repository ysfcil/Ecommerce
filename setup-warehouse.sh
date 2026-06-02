#!/bin/bash
# Volume mount SQL initialization file to postgres container
docker exec local-postgres psql -U data_engineer -d ecommerce_warehouse << 'SQL'
-- ===================================================================
-- ECOMMERCE WAREHOUSE SCHEMA
-- ===================================================================

-- Dimension: Users
CREATE TABLE IF NOT EXISTS dim_users (
    user_id BIGSERIAL PRIMARY KEY,
    user_key VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id BIGSERIAL PRIMARY KEY,
    product_key VARCHAR(50) UNIQUE NOT NULL,
    title VARCHAR(500),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Dimension: Devices
CREATE TABLE IF NOT EXISTS dim_devices (
    device_id BIGSERIAL PRIMARY KEY,
    device_type VARCHAR(50),
    os VARCHAR(50),
    browser VARCHAR(50),
    UNIQUE(device_type, os, browser)
);

-- Fact: Clickstream Events
CREATE TABLE IF NOT EXISTS fact_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_key VARCHAR(50) UNIQUE NOT NULL,
    session_key VARCHAR(50),
    user_id BIGINT REFERENCES dim_users(user_id) ON DELETE SET NULL,
    product_id BIGINT REFERENCES dim_products(product_id) ON DELETE SET NULL,
    device_id BIGINT REFERENCES dim_devices(device_id) ON DELETE SET NULL,
    event_type VARCHAR(50),
    quantity INT DEFAULT 1,
    event_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Aggregation: Daily Sales
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    date DATE PRIMARY KEY,
    total_events INT,
    total_views INT,
    total_cart_adds INT,
    total_purchases INT,
    total_revenue DECIMAL(15, 2),
    unique_users INT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Aggregation: Product Performance
CREATE TABLE IF NOT EXISTS agg_product_performance (
    product_id BIGINT REFERENCES dim_products(product_id) ON DELETE CASCADE,
    metric_date DATE,
    views INT DEFAULT 0,
    cart_adds INT DEFAULT 0,
    purchases INT DEFAULT 0,
    revenue DECIMAL(15, 2) DEFAULT 0,
    PRIMARY KEY (product_id, metric_date)
);

-- Metadata: Data Quality
CREATE TABLE IF NOT EXISTS metadata_data_quality (
    quality_id BIGSERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    check_timestamp TIMESTAMP DEFAULT NOW(),
    total_events INT,
    total_bytes BIGINT,
    throughput_bytes_per_sec DECIMAL(15, 2),
    status VARCHAR(20)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_users_email ON dim_users(email);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category);
CREATE INDEX IF NOT EXISTS idx_fact_events_user ON fact_events(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_product ON fact_events(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp ON fact_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_events_event_type ON fact_events(event_type);

-- Insert sample device data
INSERT INTO dim_devices (device_type, os, browser) VALUES
('mobile', 'iOS', 'Safari'),
('mobile', 'Android', 'Chrome'),
('desktop', 'Windows', 'Chrome'),
('desktop', 'macOS', 'Safari'),
('desktop', 'Linux', 'Firefox'),
('tablet', 'iPadOS', 'Safari'),
('tablet', 'Android', 'Chrome')
ON CONFLICT DO NOTHING;

-- Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO data_engineer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO data_engineer;

SELECT 'Warehouse schema initialized successfully!' as status;
SQL
