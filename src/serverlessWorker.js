let isWarm = false;
let coldStartTimestamp = null;

// Simulated Serverless Handler (Vercel Serverless / AWS Lambda style)
async function runInvoiceWorker(tenantId, orderId, amount) {
  const startTime = performance.now();
  let invocationType = 'WARM_START';

  if (!isWarm) {
    invocationType = 'COLD_START';
    coldStartTimestamp = new Date().toISOString();
    // Simulate runtime & dependency initialization overhead on first invocation
    await new Promise((resolve) => setTimeout(resolve, 180));
    isWarm = true;
  } else {
    // Warm invocation reuses initialized execution context
    await new Promise((resolve) => setTimeout(resolve, 12));
  }

  const durationMs = Number((performance.now() - startTime).toFixed(2));

  const invoice = {
    invoiceNumber: `INV-${tenantId.toUpperCase()}-${orderId}`,
    tenantId,
    orderId,
    amount,
    generatedAt: new Date().toISOString(),
    metrics: {
      invocationType,
      executionTimeMs: durationMs,
      containerInitializedAt: coldStartTimestamp,
    },
  };

  console.log(`[Serverless Worker] ${invocationType} | Order #${orderId} | Latency: ${durationMs}ms`);
  return invoice;
}

module.exports = { runInvoiceWorker };