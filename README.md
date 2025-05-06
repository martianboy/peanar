# Peanar — RabbitMQ‑backed Job Processing for Node.js

> A batteries‑included yet lightweight library for running background jobs and distributed task queues in Node.js & TypeScript, powered by RabbitMQ.

\[[![npm version](https://img.shields.io/npm/v/peanar.svg)](https://www.npmjs.com/package/peanar)]
\[[![Integration Tests](https://github.com/martianboy/peanar/actions/workflows/e2e.yml/badge.svg)](https://github.com/martianboy/peanar/actions)]
\[[![Unit Tests](https://github.com/martianboy/peanar/actions/workflows/unit-test.yml/badge.svg)](https://github.com/martianboy/peanar/actions)]

---

## Why Peanar?

* **RabbitMQ native** — embraces AMQP 0‑9‑1 semantics: priorities, delayed queues, dead‑lettering.
* **Resilient** — automatic reconnects and channel re‑creation recover gracefully from network or channel failures **without losing jobs**.
* **TypeScript first** — full typings for a pleasant DX.
* **Minimal surface** — one class (`PeanarApp`) does the heavy lifting.
* **Graceful workers** — cooperative shutdown, back‑pressure, priority dispatching.
* **Production‑tested** — battle‑hardened at scale for years.

---

## Installation

```bash
npm i peanar
# Or
yarn add peanar
````

Requires **Node.js 18+** and a reachable **RabbitMQ** instance.

---

## Quick start

```ts
import PeanarApp from 'peanar';

const app = new PeanarApp({
  connection: {
    hostname: 'localhost',
    username: 'guest',
    password: 'guest',
  },
  poolSize: 4,           // AMQP channels kept in the pool
});

// 1. Define a job (returns an *enqueue* function)
const sendEmail = app.job({
  name: 'sendEmail',           // Unique name of the job
  queue: 'mailer',             // The queue/routing key
  handler: async (payload: { to: string; subject: string; html: string }) => {
    await EmailService.send(payload);
  },
  max_retries: 5,              // Retry failed deliveries up to 5 times
  retry_delay: 30_000,         // Wait 30 s between attempts
  max_priority: 10,            // Declare queue with priority support
  default_priority: 5,
});

// 2. Create AMQP resources (exchanges, queues, bindings)
await app.declareAmqResources();

// 3. Start workers that will process jobs
await app.worker({
  queues: ['mailer'],   // Which queues to consume from
  concurrency: 3,       // Parallel executions
  prefetch: 1,          // Basic.qos per channel
});

// 4. Produce jobs
await sendEmail({
  to: 'hey@example.com',
  subject: 'Welcome!',
  html: '<h1>Hello there \u270c\ufe0f</h1>',
});

// 5. Shutdown gracefully on SIGTERM
process.on('SIGTERM', () => app.shutdown(15_000));
```

---

## Concepts at a glance

| Term           | Description                                                                                    |
| -------------- | ---------------------------------------------------------------------------------------------- |
| **App**        | Central object that owns the RabbitMQ connection, channel pool, registry, consumers & workers. |
| **Job**        | A unit of work defined by `queue`, `handler`, retry policy, etc.                               |
| **Enqueue fn** | Function returned by `app.job()` that publishes messages with some sugar helpers.              |
| **Worker**     | Internal transform stream that executes the job `handler` and acknowledges the message.        |

---

## Public API

### class `PeanarApp`

#### `new PeanarApp(options?)`

Create an application instance.

| Option       | Type                                                                            | Default       | Purpose                                 |                              |
| ------------ | ------------------------------------------------------------------------------- | ------------- | --------------------------------------- | ---------------------------- |
| `connection` | [`IConnectionParams`](https://github.com/amqp-ts/amqp-ts#connection-parameters) | string        | `amqp://localhost`                      | RabbitMQ connection settings |
| `poolSize`   | `number`                                                                        | `2`           | Channels kept in the internal pool      |                              |
| `prefetch`   | `number`                                                                        | `1`           | Basic.qos prefetch for every consumer   |                              |
| `jobClass`   | `typeof PeanarJob`                                                              | `PeanarJob`   | Override the runtime job implementation |                              |
| `logger`     | `(...args: any[]) => void`                                                      | `console.log` | Inject custom logging                   |                              |

---

#### `app.job(definition) ⇒ enqueueFn`

Registers a job and returns an **enqueue function**.

```ts
const enqueue = app.job(definition);
await enqueue(...args); // publish immediately
```

##### `definition` (interface `IPeanarJobDefinitionInput`)

| Field              | Type                        | Required | Description                                        |
| ------------------ | --------------------------- | -------- | -------------------------------------------------- |
| `queue`            | `string`                    | **yes**  | Name of the queue (and default routing key).       |
| `handler`          | `(...args) => Promise<any>` | **yes**  | Async function executed by workers.                |
| `name`             | `string`                    | no       | Public name (defaults to `handler.name`).          |
| `exchange`         | `string`                    | no       | Exchange to publish to (default direct "").        |
| `expires`          | `number` (ms)               | no       | Per‑message TTL.                                   |
| `max_retries`      | `number`                    | no       | How many times to retry on failure.                |
| `retry_delay`      | `number` (ms)               | no       | Wait before re‑queuing a failed message.           |
| `retry_exchange`   | `string`                    | no       | Dead‑letter exchange for retries.                  |
| `error_exchange`   | `string`                    | no       | Exchange that stores permanently failed jobs.      |
| `delayed_run_wait` | `number` (ms)               | no       | Worker sleep after consuming from a delayed queue. |
| `max_priority`     | `number` \[0 – 255]          | no       | Declare queue with priority support.               |
| `default_priority` | `number`                    | no       | Priority to use when none is supplied.             |
| `jobClass`         | `typeof PeanarJob`          | no       | Advanced: Override the runtime job implementation. |

---

#### `app.declareAmqResources(): Promise<void>`

Idempotently declares every queue, exchange & binding collected so far. Call **once, after all jobs are registered** and before you start producing or consuming.

#### `app.worker(options): Promise<void>`

Create and start one or more workers.

| Option        | Type       | Default | Description                       |
| ------------- | ---------- | ------- | --------------------------------- |
| `queues`      | `string[]` | -       | Queues to consume from            |
| `concurrency` | `number`   | `1`     | Max parallel `handler` executions |
| `prefetch`    | `number`   | `1`     | Basic.qos for these consumers     |

#### `app.call(name, argsArray)`

Low‑level helper to enqueue a job by its `name`. Useful when the enqueue function isn't in scope.

#### `app.pauseQueue(queue)` / `app.resumeQueue(queue)`

Temporarily stops or resumes consumers of a queue without shutting down the entire app. Handy for maintenance windows.

#### `app.shutdown(timeoutMs?)`

Gracefully shuts down consumers and workers, waits `timeoutMs` (default: unlimited) for in‑flight jobs, then closes the AMQP connection.

---

## Workers & concurrency

Peanar automatically spins up **Worker**s per queue and routes incoming deliveries through a Transform stream. Concurrency is configurable via worker concurrency options.

### Retries, resiliency & error handling

When a job throws, Peanar optionally delays and retries the message up to `max_retries`. If the broker connection drops or a channel closes unexpectedly, Peanar transparently reconnects, re‑creates the consumers and resumes processing — so **no job is left behind**. After the retry budget is exhausted, the message is nacked to `error_exchange` for later inspection.

---

## Examples

* `examples/publisher.ts`
* `examples/workers.ts`

Run with `ts‑node`:

```bash
npx ts-node examples/workers.ts
npx ts-node examples/publisher.ts
```

---

## Roadmap

* Persistent job storage (idempotency & deduplication)
* Monitoring UI with live queue metrics
* First‑class support for quorum & stream queues
* More...

---

## Contributing

1. Start RabbitMQ locally: `docker compose up -d rabbitmq`.
2. `npm ci && npm test && npm run test:e2e` to run the suite.
3. Send pull requests — we love them ❤.

---

## License

MIT — see [LICENSE](./LICENSE).
