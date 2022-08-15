'use strict';
const crypto = require('node:crypto');

/**
 * Stripe-style signature header: `t=<unix>,v1=<hex hmac>` over `${t}.${body}`.
 * The timestamp is signed, so receivers can reject replays outside a tolerance window.
 */
function sign(secret, body, timestamp = Math.floor(Date.now() / 1000)) {
  const mac = crypto.createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex');
  return `t=${timestamp},v1=${mac}`;
}

/** Receiver-side verification (shipped to customers as a snippet). Supports secret rotation: any v1 may match. */
function verify(secrets, body, header, toleranceSec = 300, now = Math.floor(Date.now() / 1000)) {
  const t = Number((/(?:^|,)t=(\d+)/.exec(header) || [])[1]);
  const sigs = [...header.matchAll(/(?:^|,)v1=([0-9a-f]{64})/g)].map((m) => m[1]);
  if (!t || sigs.length === 0) return { ok: false, reason: 'malformed header' };
  if (Math.abs(now - t) > toleranceSec) return { ok: false, reason: 'timestamp outside tolerance' };
  for (const secret of [].concat(secrets)) {
    const expected = Buffer.from(crypto.createHmac('sha256', secret).update(`${t}.${body}`).digest('hex'));
    if (sigs.some((s) => crypto.timingSafeEqual(Buffer.from(s), expected))) return { ok: true };
  }
  return { ok: false, reason: 'signature mismatch' };
}

module.exports = { sign, verify };
