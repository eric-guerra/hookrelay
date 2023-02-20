'use strict';
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { createDeliverer, backoffSec, classify } = require('../src/delivery');
const { memoryQueue, memoryStore } = require('../src/memory');
const { sign, verify } = require('../src/signing');
const { isPrivate, checkDestination } = require('../src/ssrf');

// A receiver whose behaviour each test scripts.
let behaviour = () => 200;
const received = [];
let server, base;
before(async () => {
  server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c)).on('end', () => {
      received.push({ headers: req.headers, body, path: req.url });
      const status = behaviour(req);
      res.writeHead(status).end(status === 200 ? 'thanks' : 'nope');
    });
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  base = `http://127.0.0.1:${server.address().port}`;
});
after(() => { server.closeAllConnections(); server.close(); });

function setup(options = {}) {
  let clock = 1_700_000_000_000;
  const now = () => clock;
  const queue = memoryQueue(now);
  const store = memoryStore([{ id: 'ep1', url: `${base}/hooks`, secret: 'whsec_test' }]);
  const d = createDeliverer({ queue, store, now, allowPrivate: true, rand: () => 1, options });
  return { d, queue, store, advance: (s) => (clock += s * 1000), now };
}

const event = { type: 'order.paid', created: 1700000000, data: { orderId: 'O-1' } };

test('delivers a signed payload the receiver can verify', async () => {
  behaviour = () => 200;
  received.length = 0;
  const { d, queue, now } = setup();
  const id = await d.enqueue('ep1', event);
  const [r] = await d.pollOnce();
  assert.equal(r.outcome, 'ok');
  assert.equal(queue.q.length, 0);
  const got = received[0];
  assert.equal(got.headers['x-hookrelay-id'], id);
  assert.deepEqual(verify(['old_secret', 'whsec_test'], got.body, got.headers['x-hookrelay-signature'], 300, Math.floor(now() / 1000)), { ok: true });
  assert.equal(JSON.parse(got.body).data.orderId, 'O-1');
});

test('retries 5xx with backoff, then succeeds', async () => {
  let calls = 0;
  behaviour = () => (++calls < 3 ? 503 : 200);
  const { d, queue, advance, store } = setup();
  await d.enqueue('ep1', event);
  assert.equal((await d.pollOnce())[0].outcome, 'retry');
  assert.deepEqual(await d.pollOnce(), [], 'not visible until backoff passes');
  advance(10);
  assert.equal((await d.pollOnce())[0].outcome, 'retry');
  advance(20);
  assert.equal((await d.pollOnce())[0].outcome, 'ok');
  assert.deepEqual(store.attempts.map((a) => a.attempt), [1, 2, 3]);
  assert.equal(queue.q.length, 0);
});

test('exhausted retries go to the DLQ; 4xx goes straight there', async () => {
  behaviour = () => 500;
  const { d, queue, advance } = setup({ maxAttempts: 2, breakerThreshold: 99 });
  await d.enqueue('ep1', event);
  await d.pollOnce();
  advance(60);
  assert.equal((await d.pollOnce())[0].outcome, 'dead');
  assert.equal(queue.dlq.length, 1);
  assert.equal(queue.dlq[0].lastError, 'HTTP 500');

  behaviour = () => 400;
  const s2 = setup();
  await s2.d.enqueue('ep1', event);
  assert.equal((await s2.d.pollOnce())[0].outcome, 'fail');
  assert.equal(s2.queue.dlq.length, 1);
});

test('410 Gone disables the endpoint', async () => {
  behaviour = () => 410;
  const { d, store } = setup();
  await d.enqueue('ep1', event);
  await d.pollOnce();
  assert.equal(store.eps.get('ep1').disabled, true);
  await d.enqueue('ep1', event);
  assert.equal((await d.pollOnce())[0].outcome, 'dropped');
});

test('circuit breaker postpones without spending attempts', async () => {
  behaviour = () => 503;
  const { d, store, advance, queue } = setup({ breakerThreshold: 2, breakerCooldownSec: 120 });
  await d.enqueue('ep1', event);
  await d.enqueue('ep1', event);
  await d.pollOnce(); // both fail → breaker opens at 2
  assert.ok(store.breakers.get('ep1').openUntil > 0);
  advance(30);
  const before = store.attempts.length;
  const rs = await d.pollOnce();
  assert.ok(rs.every((r) => r.outcome === 'postponed'));
  assert.equal(store.attempts.length, before, 'no HTTP calls while open');
  behaviour = () => 200;
  advance(200);
  const after = await d.pollOnce();
  assert.ok(after.every((r) => r.outcome === 'ok'));
  assert.equal(queue.q.length, 0);
  assert.equal(store.breakers.get('ep1').failures, 0);
});

test('duplicate queue deliveries are not re-sent', async () => {
  behaviour = () => 200;
  received.length = 0;
  const { d, queue } = setup();
  await d.enqueue('ep1', event);
  const body = structuredClone(queue.q[0].body);
  await d.pollOnce();
  queue.duplicate(body);
  assert.equal((await d.pollOnce())[0].outcome, 'duplicate');
  assert.equal(received.length, 1);
});

test('signature verification rejects tampering and replays', () => {
  const h = sign('s', '{"a":1}', 1000);
  assert.equal(verify('s', '{"a":2}', h, 300, 1000).reason, 'signature mismatch');
  assert.equal(verify('s', '{"a":1}', h, 300, 2000).reason, 'timestamp outside tolerance');
  assert.equal(verify('s', '{"a":1}', 'garbage', 300, 1000).reason, 'malformed header');
});

test('backoff and classification', () => {
  assert.equal(backoffSec(1, { baseDelaySec: 10, maxDelaySec: 3600 }, () => 1), 10);
  assert.equal(backoffSec(4, { baseDelaySec: 10, maxDelaySec: 3600 }, () => 1), 80);
  assert.equal(backoffSec(20, { baseDelaySec: 10, maxDelaySec: 3600 }, () => 1), 3600);
  assert.equal(backoffSec(3, { baseDelaySec: 10, maxDelaySec: 3600 }, () => 0), 1);
  assert.deepEqual([200, 204, 301, 404, 408, 429, 503, null].map(classify), ['ok', 'ok', 'fail', 'fail', 'retry', 'retry', 'retry', 'retry']);
});

test('SSRF guard blocks private and metadata addresses', async () => {
  for (const ip of ['10.1.2.3', '127.0.0.1', '169.254.169.254', '192.168.1.1', '172.20.0.1', '::1', '::ffff:10.0.0.1', 'fd00::1'])
    assert.equal(isPrivate(ip), true, ip);
  for (const ip of ['8.8.8.8', '172.32.0.1', '2606:4700::1111']) assert.equal(isPrivate(ip), false, ip);

  assert.equal((await checkDestination('http://example.com/x')).reason, 'https required');
  assert.equal((await checkDestination('https://user:pw@example.com/')).reason, 'credentials in URL not allowed');
  const rebind = await checkDestination('https://evil.example/', { lookup: async () => [{ address: '169.254.169.254' }] });
  assert.match(rebind.reason, /private address/);
  assert.equal((await checkDestination('https://ok.example/', { lookup: async () => [{ address: '93.184.216.34' }] })).ok, true);
});
