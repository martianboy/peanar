import PeanarApp = require('../src');

function dummy() {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, 4000);
  });
}

const app = new PeanarApp({
  prefetch: 1
});

let interval: NodeJS.Timer;

let enqueue_counter = 0

async function main() {
  const enqueueDummy = app.job({
    handler: dummy,
    queue: 'dummy'
  });

  await app.declareAmqResources();

  // await Promise.all(
  //   Array(4)
  //     .fill(0)
  //     .map(() => enqueueDummy())
  // );

  await app.worker({
    queues: ['dummy'],
    concurrency: 3,
    prefetch: 1
  });

  // interval = setInterval(() => {
  //   enqueue_counter += 1;
  //   console.log(`enqueue #${enqueue_counter}`);
  //   enqueueDummy();
  // }, 200);
  // await Promise.all(
  //   Array(4)
  //     .fill(0)
  //     .map(() => enqueueDummy())
  // );

  // app.shutdown(20000);
}

async function shutdown() {
  // clearInterval(interval);
  await app.shutdown(20000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().then(() => {}, ex => console.error(ex));
