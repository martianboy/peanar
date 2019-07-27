import PeanarApp = require("../src");

function dummy() {
    return new Promise((resolve, reject) => {
        setTimeout(resolve, 1500);
    });
}

async function main() {
    const app = new PeanarApp;

    const enqueueDummy = app.job(dummy, {
        queue: 'dummy'
    });

    await Promise.all(Array(4).fill(0).map(() => enqueueDummy()));

    await app.worker({
        queues: ['dummy'],
        concurrency: 4,
        prefetch: 1
    });

    await Promise.all(Array(4).fill(0).map(() => enqueueDummy()));

    app.shutdown(20000);
}

main().then(() => {}, ex => console.error(ex));