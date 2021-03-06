import PeanarApp = require('../src');

const processed = Array(2000).fill(JSON.stringify({
  matrix_unique_id: '173048987',
  listing_mui: 172919758,
  description: '',
  order: 0,
  mls: 'HAR'
})).join('')

const payload = {
  id: '18ed772f-99cf-47fc-8ca8-be7c14fed134',
  name: 'HAR.upload_photos',
  args: [
    {
      id: '173048987',
      created_at: '2019-09-28T19:48:22.349Z',
      value: {
        Type: 'Image',
        Order: '0',
        Table_MUI: '172919758',
        UploadDate: '2019-04-30T20:29:02.830',
        Description: '',
        ModifiedDate: '2019-04-30T20:29:02.830',
        matrix_unique_id: '173048987',
        ParentProviderKey: ''
      },
      class: null,
      resource: 'Media',
      matrix_unique_id: 173048987,
      revision: 1,
      matrix_modified_dt: '2019-04-30T20:29:02.830Z',
      mls: 'HAR',
      processed
    }
  ]
};

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
  const delay = []

  for (let i = 0; i < 1000; i++) {
    const [s0, n0] = process.hrtime()
    await Promise.all(Array(20).fill(0).map(() => enqueueDummy(payload)));
    const [s1, n1] = process.hrtime()

    delay.push((s1 - s0) * 1000 + (n1 - n0) * 1e-6)
  }

  console.log(Math.min(...delay))
  console.log(Math.max(...delay))
  await new Promise(res => setTimeout(res, 1000));
  await shutdown()
}

async function shutdown() {
  await app.shutdown(20000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().then(() => {}, ex => console.error(ex));
