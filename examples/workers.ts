import PeanarApp = require("../src");

function dummy() {
    return new Promise((resolve, reject) => {
        setTimeout(resolve, 500);
    });
}

async function main() {
    const app = new PeanarApp

    const enqueueDummy = app.job(dummy, {
        queue: 'dummy'
    });

    await enqueueDummy();
    await enqueueDummy();
    await enqueueDummy();
    await enqueueDummy();

    await app.worker({
        queues: ['dummy'],
        concurrency: 10
    });

    await enqueueDummy();
    await enqueueDummy();
    await enqueueDummy();
    await enqueueDummy();
}

main().then(() => {}, ex => console.error(ex));