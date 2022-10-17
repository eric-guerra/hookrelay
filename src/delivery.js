'use strict';
const crypto = require('node:crypto');
const { sign } = require('./signing');
const { checkDestination } = require('./ssrf');

/**
 * Delivery engine, independent of AWS: `queue` and `store` are interfaces implemented by SQS/DynamoDB
 * in production (src/aws.js) and in-memory in tests (src/memory.js).
 *
 *   queue: send(msg, delaySec), receive(max) -> [{ body, receipt }], ack(receipt), sendDlq(msg)
 *   store: getEndpoint(id), putAttempt(rec), getBreaker(id), putBreaker(id, state), incrementBreakerFailures(id)->n,
 *          markDelivered(deliveryId), isDelivered(deliveryId)
 */

const DEFAULTS = {
  maxAttempts: 8,
  baseDelaySec: 10,
  maxDelaySec: 6 * 3600,
  timeoutMs: 10_000,
  breakerThreshold: 5, // consecutive failures
  breakerCooldownSec: 300,
  maxResponseBytes: 4096,
};

/** Full-jitter exponential backoff (AWS architecture blog): random in [0, min(cap, base*2^n)]. */
function backoffSec(attempt, o = DEFAULTS, rand = Math.random) {
  const ceiling = Math.min(o.maxDelaySec, o.baseDelaySec * 2 ** (attempt - 1));
  return Math.max(1, Math.round(rand() * ceiling));
}

/** 408/429/5xx and network errors are retryable; other 4xx means the receiver rejected it for good. */
function classify(status) {
  if (status === null) return 'retry';
  if (status >= 200 && status < 300) return 'ok';
  if (status === 408 || status === 429 || status >= 500) return 'retry';
  if (status === 410) return 'gone'; // receiver says: stop sending to this endpoint
  return 'fail';
}

function createDeliverer({ queue, store, fetchImpl = fetch, now = () => Date.now(), options = {}, allowPrivate = false, rand = Math.random, log = () => {} }) {
  const o = { ...DEFAULTS, ...options };

  /** Producer API: enqueue one event for one endpoint. */
  async function enqueue(endpointId, event) {
    const msg = { deliveryId: crypto.randomUUID(), endpointId, event, attempt: 1, firstAt: now() };
    await queue.send(msg, 0);
    return msg.deliveryId;
  }

  async function attemptOnce(msg) {
    const ep = await store.getEndpoint(msg.endpointId);
    if (!ep || ep.disabled) return { outcome: 'dropped', reason: 'endpoint missing or disabled' };

    const breaker = (await store.getBreaker(ep.id)) || { failures: 0, openUntil: 0 };
    if (breaker.openUntil > now()) {
      // Don't burn an attempt while the breaker is open; just postpone.
      return { outcome: 'postponed', delaySec: Math.ceil((breaker.openUntil - now()) / 1000) };
    }

    const dest = await checkDestination(ep.url, { allowPrivate });
    if (!dest.ok) return { outcome: 'fail', reason: dest.reason, status: null };

    const body = JSON.stringify({ id: msg.deliveryId, type: msg.event.type, created: msg.event.created, data: msg.event.data });
    const headers = {
      'content-type': 'application/json',
      'user-agent': 'HookRelay/1.0',
      'x-hookrelay-id': msg.deliveryId, // receivers dedupe on this
      'x-hookrelay-attempt': String(msg.attempt),
      'x-hookrelay-signature': sign(ep.secret, body, Math.floor(now() / 1000)),
    };
    const started = now();
    let status = null, error = null, snippet = '';
    try {
      const res = await fetchImpl(ep.url, { method: 'POST', headers, body, redirect: 'manual', signal: AbortSignal.timeout(o.timeoutMs) });
      status = res.status;
      snippet = (await res.text()).slice(0, o.maxResponseBytes);
    } catch (e) {
      error = e.name === 'TimeoutError' ? `timeout after ${o.timeoutMs}ms` : e.message;
    }
    await store.putAttempt({ deliveryId: msg.deliveryId, attempt: msg.attempt, endpointId: ep.id, status, error, ms: now() - started, response: snippet, at: now() });

    const c = classify(status);
    // Circuit breaker on consecutive failures per endpoint.
    if (c === 'ok') await store.putBreaker(ep.id, { failures: 0, openUntil: 0 });
    else if (c === 'retry') {
      // Atomic increment: concurrent workers failing against the same endpoint must all count.
      const failures = await store.incrementBreakerFailures(ep.id);
      if (failures >= o.breakerThreshold) {
        log({ msg: 'breaker open', endpoint: ep.id, failures });
        await store.putBreaker(ep.id, { failures, openUntil: now() + o.breakerCooldownSec * 1000 });
      }
    }
    return { outcome: c, status, reason: error || (status ? `HTTP ${status}` : '') };
  }

  /** Handle one queue message; always acks the original (retries are re-enqueued with delay). */
  async function handle(msg) {
    const r = await attemptOnce(msg);
    switch (r.outcome) {
      case 'ok':
        await store.markDelivered(msg.deliveryId);
        return r;
      case 'postponed':
        await queue.send(msg, Math.min(900, r.delaySec)); // SQS max delay is 15 min
        return r;
      case 'retry':
        if (msg.attempt < o.maxAttempts) {
          await queue.send({ ...msg, attempt: msg.attempt + 1 }, Math.min(900, backoffSec(msg.attempt, o, rand)));
          return r;
        }
        await queue.sendDlq({ ...msg, lastError: r.reason });
        return { ...r, outcome: 'dead' };
      case 'gone':
        await store.disableEndpoint?.(msg.endpointId, 'receiver returned 410');
        await queue.sendDlq({ ...msg, lastError: r.reason });
        return r;
      default:
        await queue.sendDlq({ ...msg, lastError: r.reason });
        return r;
    }
  }

  /** One poll cycle. Duplicate SQS deliveries of an already-delivered id are skipped. */
  async function pollOnce(max = 10) {
    const msgs = await queue.receive(max);
    const results = await Promise.all(msgs.map(async ({ body, receipt }) => {
      try {
        if (await store.isDelivered(body.deliveryId)) return { outcome: 'duplicate' };
        return await handle(body);
      } finally {
        await queue.ack(receipt);
      }
    }));
    return results;
  }

  return { enqueue, handle, pollOnce, attemptOnce };
}

module.exports = { createDeliverer, backoffSec, classify, DEFAULTS };
