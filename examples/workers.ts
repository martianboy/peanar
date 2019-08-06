import PeanarApp = require('../src');

function dummy() {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, 1500);
  });
}

const app = new PeanarApp({
  prefetch: 1
});

async function main() {
  const enqueueDummy = app.job({
    handler: dummy,
    queue: 'dummy'
  });

  await app.declareAmqResources();

  await Promise.all(
    Array(4)
      .fill(0)
      .map(() => enqueueDummy())
  );

  await app.worker({
    queues: ['dummy'],
    concurrency: 4,
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
