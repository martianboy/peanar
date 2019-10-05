import PeanarApp = require('../src');

function dummy2() {
  return new Promise((resolve, reject) => {
    if (Math.random() > 0.033333) {
      reject(new Error('Oh nooooo!'));
    } else {
      setTimeout(resolve, 1500);
    }
  });
}

const app = new PeanarApp({
  prefetch: 1
});

const enqueueDummy = app.job({
  handler: dummy2,
  queue: 'dummy2',
  // retry_delay: 5,
  max_retries: 5,
  retry_exchange: 'dummy2.retry'
});

async function main() {
  await app.declareAmqResources();
  await enqueueDummy();

  await app.worker({
    queues: ['dummy2'],
    prefetch: 1
  });

  // await Promise.all(
  //   Array(4)
  //     .fill(0)
  //     .map(() => enqueueDummy())
  // );

  // app.shutdown(20000);
}

async function shutdown() {
  await app.shutdown(20000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().then(() => {}, ex => console.error(ex));
