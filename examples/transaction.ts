import PeanarApp = require('../src');

function dummy() {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, 1500);
  });
}

const app = new PeanarApp({
  prefetch: 1
});

const enqueueDummy = app.job({
  handler: dummy,
  queue: 'dummy'
});

async function main() {
  await app.declareAmqResources();

  const t = enqueueDummy.transaction();
  await t.begin();
  await t.call();
  await t.commit();

  // await new Promise(res => setTimeout(res, 100000));
  await shutdown()
}

async function shutdown() {
  await app.shutdown(20000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().then(() => {}, ex => console.error(ex));
