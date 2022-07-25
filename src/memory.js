'use strict';
// In-memory queue/store with SQS-like delay semantics, driven by an injectable clock.

function memoryQueue(now) {
  const q = [];
  const dlq = [];
  let seq = 0;
  return {
    q, dlq,
    async send(body, delaySec) { q.push({ body: structuredClone(body), visibleAt: now() + delaySec * 1000, receipt: `r${++seq}` }); },
    async receive(max) {
      const ready = q.filter((m) => m.visibleAt <= now() && !m.inFlight).slice(0, max);
      ready.forEach((m) => (m.inFlight = true));
      return ready.map((m) => ({ body: m.body, receipt: m.receipt }));
    },
    async ack(receipt) { const i = q.findIndex((m) => m.receipt === receipt); if (i >= 0) q.splice(i, 1); },
    async sendDlq(body) { dlq.push(body); },
    /** Test helper: redeliver a message as SQS sometimes does. */
    duplicate(body) { q.push({ body: structuredClone(body), visibleAt: 0, receipt: `r${++seq}` }); },
  };
}

function memoryStore(endpoints = []) {
  const eps = new Map(endpoints.map((e) => [e.id, { ...e }]));
  const attempts = [];
  const breakers = new Map();
  const delivered = new Set();
  return {
    attempts, breakers, eps,
    async getEndpoint(id) { return eps.get(id); },
    async disableEndpoint(id, reason) { const e = eps.get(id); if (e) Object.assign(e, { disabled: true, disabledReason: reason }); },
    async putAttempt(a) { attempts.push(a); },
    async getBreaker(id) { return breakers.get(id); },
    async putBreaker(id, s) { breakers.set(id, s); },
    async incrementBreakerFailures(id) { const b = breakers.get(id) || { failures: 0, openUntil: 0 }; b.failures += 1; breakers.set(id, b); return b.failures; },
    async markDelivered(id) { delivered.add(id); },
    async isDelivered(id) { return delivered.has(id); },
  };
}

module.exports = { memoryQueue, memoryStore };
