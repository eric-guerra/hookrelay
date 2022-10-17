'use strict';
const { sqsQueue, dynamoStore } = require('./aws');
const { createDeliverer } = require('./delivery');

async function main() {
  const d = createDeliverer({
    queue: sqsQueue({ queueUrl: process.env.QUEUE_URL, dlqUrl: process.env.DLQ_URL }),
    store: dynamoStore({ table: process.env.TABLE }),
    log: (o) => console.log(JSON.stringify(o)),
  });
  let stopping = false;
  process.on('SIGTERM', () => { stopping = true; });
  while (!stopping) {
    try {
      const results = await d.pollOnce(10);
      for (const r of results) if (r.outcome !== 'ok') console.log(JSON.stringify({ msg: 'delivery', ...r }));
    } catch (e) {
      console.error(JSON.stringify({ msg: 'poll failed', error: e.message }));
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

main();
