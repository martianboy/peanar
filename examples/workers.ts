import PeanarApp from '../src';

const timeouts: NodeJS.Timeout[] = [];

function dummy() {
  console.log('Dummy job started');
  return new Promise<void>((resolve, reject) => {
    timeouts.push(setTimeout(() => {
      console.log('Dummy job done');
      resolve();
    }, 120000));
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
  console.log('Shutting down...');
  for (const timeout of timeouts) {
    clearTimeout(timeout);
  }
  await app.shutdown(20000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().then(() => {}, ex => console.error(ex));
