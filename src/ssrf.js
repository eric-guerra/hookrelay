'use strict';
const dns = require('node:dns/promises');
const net = require('node:net');

// Customers register arbitrary URLs; without this, a webhook to http://169.254.169.254/ reads cloud credentials.
const BLOCKED_V4 = [
  ['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8], ['169.254.0.0', 16],
  ['172.16.0.0', 12], ['192.168.0.0', 16], ['198.18.0.0', 15], ['224.0.0.0', 4], ['240.0.0.0', 4],
];

const toInt = (ip) => ip.split('.').reduce((a, o) => (a << 8) + Number(o), 0) >>> 0;

function isPrivate(ip) {
  if (net.isIPv4(ip)) {
    const n = toInt(ip);
    return BLOCKED_V4.some(([base, bits]) => (n >>> (32 - bits)) === (toInt(base) >>> (32 - bits)));
  }
  const v6 = ip.toLowerCase();
  if (v6.startsWith('::ffff:')) return isPrivate(v6.slice(7));
  return v6 === '::1' || v6 === '::' || v6.startsWith('fc') || v6.startsWith('fd') || v6.startsWith('fe80');
}

/**
 * Validate a destination at registration *and* at send time (DNS can change: rebinding).
 * `allowPrivate` exists for tests and on-prem installs.
 */
async function checkDestination(url, { allowPrivate = false, lookup = dns.lookup } = {}) {
  let u;
  try { u = new URL(url); } catch { return { ok: false, reason: 'invalid URL' }; }
  if (u.protocol !== 'https:' && !(allowPrivate && u.protocol === 'http:')) return { ok: false, reason: 'https required' };
  if (u.username || u.password) return { ok: false, reason: 'credentials in URL not allowed' };
  if (allowPrivate) return { ok: true };
  const host = u.hostname.replace(/^\[|\]$/g, '');
  const addrs = net.isIP(host) ? [{ address: host }] : await lookup(host, { all: true });
  const bad = addrs.find((a) => isPrivate(a.address));
  return bad ? { ok: false, reason: `resolves to private address ${bad.address}` } : { ok: true };
}

module.exports = { isPrivate, checkDestination };
