const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const jwt = require('jsonwebtoken');
const path = require('path');
require('dotenv').config();

const pool = require('./db');
const { apiLimiter, tenantMiddleware, requireRole } = require('./middleware');
const { publishOrderEvent } = require('./kafka');
const { runInvoiceWorker } = require('./serverlessWorker');

const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));
app.use('/api/', apiLimiter);

// 1. Auth Login / Token Generator (Tenant A ya Tenant B ke liye JWT banata hai)
app.post('/api/v1/auth/login', (req, res) => {
  const { tenant_id, role = 'admin', email = 'user@saas.com' } = req.body;
  if (!tenant_id) return res.status(400).json({ error: 'tenant_id is required' });

  const accessToken = jwt.sign({ email, tenant_id, role }, process.env.JWT_SECRET, { expiresIn: '1h' });
  const refreshToken = jwt.sign({ email, tenant_id, role, type: 'refresh' }, process.env.JWT_SECRET, { expiresIn: '7d' });

  res.json({ accessToken, refreshToken, tenant_id, role });
});

// 2. Get Tenant Products (Strict tenant_id isolation)
app.get('/api/v1/products', tenantMiddleware, async (req, res) => {
  const { rows } = await pool.query(
    'SELECT id, tenant_id, name, price, stock FROM products WHERE tenant_id = $1 ORDER BY id ASC',
    [req.tenantId]
  );
  res.json({ tenant: req.tenantId, products: rows });
});

// 3. Get Tenant Orders - OPTIMIZED Single JOIN Query (No N+1)
app.get('/api/v1/orders', tenantMiddleware, async (req, res) => {
  const start = performance.now();
  const { rows } = await pool.query(
    `SELECT o.id, o.tenant_id, o.customer_name, o.status, o.total_amount, o.created_at,
            COALESCE(json_agg(
              json_build_object('product_id', oi.product_id, 'quantity', oi.quantity, 'price', oi.price)
            ) FILTER (WHERE oi.id IS NOT NULL), '[]') AS items
     FROM orders o
     LEFT JOIN order_items oi ON o.id = oi.order_id AND oi.tenant_id = $1
     WHERE o.tenant_id = $1
     GROUP BY o.id
     ORDER BY o.created_at DESC
     LIMIT 20`,
    [req.tenantId]
  );
  const latencyMs = Number((performance.now() - start).toFixed(2));
  res.json({ queryType: 'OPTIMIZED_SINGLE_JOIN', latencyMs, tenant: req.tenantId, orders: rows });
});

// 4. N+1 Slow Query vs Optimized JOIN Benchmark Endpoint (For Live Viva Demo!)
app.get('/api/v1/benchmark/n-plus-one', tenantMiddleware, async (req, res) => {
  // A: Un-optimized N+1 Query (1 query for orders + N queries for order_items)
  const startSlow = performance.now();
  const ordersRes = await pool.query(
    'SELECT * FROM orders WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 25',
    [req.tenantId]
  );
  const nPlusOneResults = [];
  for (const order of ordersRes.rows) {
    const itemsRes = await pool.query(
      'SELECT * FROM order_items WHERE order_id = $1 AND tenant_id = $2',
      [order.id, req.tenantId]
    );
    nPlusOneResults.push({ ...order, items: itemsRes.rows });
  }
  const slowMs = Number((performance.now() - startSlow).toFixed(2));

  // B: Optimized Single JOIN Query
  const startFast = performance.now();
  const fastRes = await pool.query(
    `SELECT o.*, COALESCE(json_agg(oi.*) FILTER (WHERE oi.id IS NOT NULL), '[]') AS items
     FROM orders o
     LEFT JOIN order_items oi ON o.id = oi.order_id AND oi.tenant_id = $1
     WHERE o.tenant_id = $1
     GROUP BY o.id
     ORDER BY o.created_at DESC
     LIMIT 25`,
    [req.tenantId]
  );
  const fastMs = Number((performance.now() - startFast).toFixed(2));

  res.json({
    tenant: req.tenantId,
    ordersChecked: ordersRes.rows.length,
    nPlusOne: { queriesExecuted: ordersRes.rows.length + 1, latencyMs: slowMs },
    optimizedJoin: { queriesExecuted: 1, latencyMs: fastMs },
    speedupFactor: `${(slowMs / Math.max(fastMs, 0.1)).toFixed(1)}x faster`,
  });
});

// 5. Place Order -> Saves Pending Order + Publishes to Kafka + Returns 202 Accepted
app.post('/api/v1/orders', tenantMiddleware, async (req, res) => {
  const { customer_name = 'Walk-in B2B Client', items } = req.body;
  if (!items || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'Order items array is required' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    let totalAmount = 0;
    for (const item of items) {
      // Verify product belongs to THIS tenant only!
      const prodRes = await client.query(
        'SELECT price, stock FROM products WHERE id = $1 AND tenant_id = $2',
        [item.productId, req.tenantId]
      );
      if (prodRes.rows.length === 0) {
        throw new Error(`Product ID ${item.productId} not found for tenant ${req.tenantId}`);
      }
      totalAmount += Number(prodRes.rows[0].price) * item.quantity;
    }

    const orderInsert = await client.query(
      `INSERT INTO orders (tenant_id, customer_name, status, total_amount)
       VALUES ($1, $2, 'PENDING_KAFKA', $3) RETURNING id, created_at`,
      [req.tenantId, customer_name, totalAmount]
    );
    const orderId = orderInsert.rows[0].id;

    for (const item of items) {
      const prodRes = await client.query('SELECT price FROM products WHERE id = $1', [item.productId]);
      await client.query(
        `INSERT INTO order_items (order_id, tenant_id, product_id, quantity, price)
         VALUES ($1, $2, $3, $4, $5)`,
        [orderId, req.tenantId, item.productId, item.quantity, prodRes.rows[0].price]
      );
    }

    await client.query('COMMIT');

    // Publish event asynchronously to Kafka topic 'tenant.orders'
    await publishOrderEvent(req.tenantId, { orderId, customer_name, totalAmount, items });

    // Trigger Serverless Invoice Worker asynchronously
    const invoicePreview = await runInvoiceWorker(req.tenantId, orderId, totalAmount);

    // Return 202 Accepted immediately
    return res.status(202).json({
      status: '202 Accepted',
      message: 'Order queued in Kafka topic tenant.orders for async inventory processing',
      orderId,
      tenantId: req.tenantId,
      totalAmount,
      serverlessInvoice: invoicePreview,
    });
  } catch (err) {
    await client.query('ROLLBACK');
    return res.status(400).json({ error: err.message });
  } finally {
    client.release();
  }
});

// 6. Cross-Tenant Isolation Test Endpoint (Fetching specific order by ID)
app.get('/api/v1/orders/:id', tenantMiddleware, async (req, res) => {
  const { rows } = await pool.query(
    'SELECT * FROM orders WHERE id = $1 AND tenant_id = $2',
    [req.params.id, req.tenantId]
  );
  if (rows.length === 0) {
    return res.status(404).json({
      error: `Order #${req.params.id} does not exist or belongs to another tenant (Tenant Isolation Enforced)`,
    });
  }
  res.json(rows[0]);
});

// 7. RBAC Protected Route (Only 'admin' role allowed)
app.delete('/api/v1/orders/:id', tenantMiddleware, requireRole('admin'), async (req, res) => {
  const result = await pool.query(
    'DELETE FROM orders WHERE id = $1 AND tenant_id = $2 RETURNING id',
    [req.params.id, req.tenantId]
  );
  if (result.rowCount === 0) return res.status(404).json({ error: 'Order not found in your tenant' });
  res.json({ deletedOrderId: req.params.id, tenantId: req.tenantId });
});

// 8. Dedicated Serverless Cold-Start vs Warm-Start Test Endpoint
app.post('/api/v1/serverless/invoice', tenantMiddleware, async (req, res) => {
  const { orderId = 999, amount = 5000 } = req.body;
  const result = await runInvoiceWorker(req.tenantId, orderId, amount);
  res.json(result);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`[API Server] Running on http://localhost:${PORT}`);
});
module.exports = app;