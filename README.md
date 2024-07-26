# HookRelay

Portfolio reference implementation of a 2022 project, built to show reliable outbound webhooks for an e-commerce platform. Merchants register endpoints, and order, payment and shipment events get to them reliably even when their servers are flaky.

**Stack:** Node.js, AWS SQS (main queue + DLQ), DynamoDB (single table, TTL), AWS SDK v3.

## How a delivery works
1. `enqueue(endpointId, event)` puts a message on SQS with a unique `deliveryId`.
2. A worker long-polls SQS and POSTs the signed payload with a 10-second timeout. Redirects are not followed.
3. The outcome is classified from the response:

   | Response | Action |
   |---|---|
   | 2xx | delivered (recorded for dedupe) |
   | 408, 429, 5xx or network error | re-enqueued with **full-jitter exponential backoff** (10 s → 6 h cap), up to 8 attempts, then DLQ |
   | 410 Gone | endpoint disabled, message goes to the DLQ |
   | other 4xx | DLQ right away (retrying won't help) |

4. Every attempt is stored (status, latency, the first 4 KB of the response) so merchants can debug their side.

## Safety and reliability
- **Signatures:**
  - Header format: `x-hookrelay-signature: t=<unix>,v1=<hmac-sha256(t.body)>`.
  - The timestamp is signed, so receivers can reject replays (5-minute tolerance).
  - Secret rotation is supported: a receiver can check several secrets.
  - `src/signing.js#verify` is the receiver-side snippet.
- **Circuit breaker per endpoint:** after 5 consecutive failures, deliveries are postponed for 5 minutes. This doesn't spend retry attempts, and it stops a dead merchant server from eating worker capacity.
  - The failure count is an **atomic increment** (DynamoDB `ADD`). Concurrent workers previously overwrote each other's counts, so the breaker never opened. A test reproduces that race.
- **SSRF guard:** destinations must use HTTPS and can't include credentials in the URL.
  - The DNS answer is checked at send time against private, loopback, link-local (cloud metadata) and CGNAT ranges, including IPv6 and IPv4-mapped addresses. This also defends against DNS rebinding.
- **Duplicates:** SQS is at-least-once. Already-delivered ids are skipped, and receivers get `x-hookrelay-id` to dedupe on their side too.

## Test
```bash
npm install && npm test
```
9 tests against a real local HTTP receiver and an in-memory SQS/DynamoDB with a controllable clock. They cover signing and verification, retry with backoff, the DLQ, 410 handling, the breaker (including concurrency), duplicate suppression, and the SSRF guard.
