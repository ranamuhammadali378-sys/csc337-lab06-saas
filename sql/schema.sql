-- 1. Tables Creation
CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(50) REFERENCES tenants(id),
    name VARCHAR(100) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    stock INT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(50) REFERENCES tenants(id),
    customer_name VARCHAR(100) NOT NULL,
    status VARCHAR(30) DEFAULT 'PENDING',
    total_amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id) ON DELETE CASCADE,
    tenant_id VARCHAR(50) REFERENCES tenants(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL
);

-- 2. Insert Two Distinct Tenants (Tenant A & Tenant B)
INSERT INTO tenants (id, name) VALUES 
('tenant-a', 'Ali Agri Traders (Tenant A)'),
('tenant-b', 'Bilal Tech Distributors (Tenant B)')
ON CONFLICT DO NOTHING;

-- 3. Insert Products for Both Tenants
INSERT INTO products (tenant_id, name, price, stock) VALUES
('tenant-a', 'Organic Fertilizer 50kg', 4500, 100),
('tenant-a', 'Hybrid Wheat Seed Bag', 3200, 80),
('tenant-b', 'Industrial Router X1', 15000, 50),
('tenant-b', 'Cat6 Cable Roll', 8500, 40);

-- 4. Seed 5000 Dummy Orders (Taake EXPLAIN ANALYZE mein clear farq nazar aaye)
INSERT INTO orders (tenant_id, customer_name, status, total_amount, created_at)
SELECT 
    CASE WHEN g % 2 = 0 THEN 'tenant-a' ELSE 'tenant-b' END,
    'Customer ' || g,
    'COMPLETED',
    (random() * 10000 + 1000)::numeric(10,2),
    NOW() - (g || ' minutes')::interval
FROM generate_series(1, 5000) g;

-- ==========================================================
-- VIVA & README BENCHMARKING COMMANDS (Run in SQL Editor)
-- ==========================================================
-- STEP A: Run BEFORE Index (Screenshot lo)
-- EXPLAIN ANALYZE SELECT * FROM orders WHERE tenant_id = 'tenant-a' ORDER BY created_at DESC LIMIT 20;

-- STEP B: Create Composite Index
-- CREATE INDEX idx_orders_tenant_created ON orders(tenant_id, created_at DESC);

-- STEP C: Run AFTER Index (Screenshot lo)
-- EXPLAIN ANALYZE SELECT * FROM orders WHERE tenant_id = 'tenant-a' ORDER BY created_at DESC LIMIT 20;