const rateLimit = require('express-rate-limit');
const jwt = require('jsonwebtoken');

// Rate Limiting (Viva checklist item)
const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  message: { error: 'Too many requests, please try again later.' },
});

// Tenant Resolution & Strict Isolation Middleware
function tenantMiddleware(req, res, next) {
  let tenantId = req.headers['x-tenant-id'];

  // Agar JWT token bheja gaya hai to us se bhi tenant_id aur role nikal lo
  const authHeader = req.headers['authorization'];
  if (authHeader && authHeader.startsWith('Bearer ')) {
    try {
      const token = authHeader.split(' ')[1];
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      req.user = decoded;
      tenantId = tenantId || decoded.tenant_id;

      // Cross-tenant spoofing check: Token Tenant A ka ho aur header Tenant B ka to block karo!
      if (decoded.tenant_id && req.headers['x-tenant-id'] && decoded.tenant_id !== req.headers['x-tenant-id']) {
        return res.status(403).json({
          error: 'TENANT_ISOLATION_VIOLATION: Token tenant does not match x-tenant-id header!',
        });
      }
    } catch (err) {
      return res.status(401).json({ error: 'Invalid or expired JWT token' });
    }
  }

  if (!tenantId) {
    return res.status(400).json({ error: 'Missing tenant context (x-tenant-id header or JWT claim required)' });
  }

  req.tenantId = tenantId;
  next();
}

// RBAC Check (e.g., sirf 'admin' hi delete ya sensitive action kar sake)
function requireRole(role) {
  return (req, res, next) => {
    const userRole = req.user?.role || req.headers['x-user-role'] || 'member';
    if (userRole !== role) {
      return res.status(403).json({
        error: `RBAC_REJECTED: Required role '${role}', but got '${userRole}'`,
      });
    }
    next();
  };
}

module.exports = { apiLimiter, tenantMiddleware, requireRole };