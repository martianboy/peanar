import PeanarApp = require('../src');

async function dummy2() {
  console.log('Hey!');
}

const app = new PeanarApp({
  prefetch: 1
});

const enqueueDummy = app.job({
  handler: dummy2,
  queue: 'dummy2',
  delayed_run_wait: 5_000,
  max_retries: 5,
  retry_exchange: 'dummy2.retry'
});

async function main() {
  await app.declareAmqResources();
  await enqueueDummy.delayed();

  await app.worker({
    queues: ['dummy2'],
    prefetch: 1
  });
}

async function shutdown() {
  await app.shutdown(20_000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().then(() => {}, ex => console.error(ex));
