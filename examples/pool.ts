import PeanarApp = require('../src');
import { Writable, TransformCallback } from 'stream';

const app = new PeanarApp({
  prefetch: 1
});

async function main() {
  await app.broker.connect();

  console.log('------------------------------------')
  const consumer = await app.broker.consume('movies');
  consumer.pipe(new Writable({
    objectMode: true,
    write: (result: unknown, _encoding: string, cb: TransformCallback) => {
      console.log(result);
      cb();
    }
  }));

  await new Promise(res => setTimeout(res, 2000));

  app.broker.pool?.acquireAndRun(async ch => {
    await ch.bindQueue("gholi", "mamali", "*").catch(ex => console.error(ex))
  })

  await new Promise(res => setTimeout(res, 6000));

  await shutdown()
}

async function shutdown() {
  await app.shutdown(20000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().then(() => {}, ex => console.error(ex));
