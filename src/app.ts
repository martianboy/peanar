import uuid from 'uuid';
import debugFn from 'debug';
const debug = debugFn('peanar');

import { PeanarInternalError, PeanarJobError } from './exceptions';
import Broker from './broker';
import Worker, { IWorkerResult } from './worker';
import PeanarJob from './job';
import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';
import { Writable, TransformCallback } from 'stream';
import Consumer from 'ts-amqp/dist/classes/Consumer';
import { IBasicProperties } from 'ts-amqp/dist/interfaces/Protocol';
import { IQueueArgs } from 'ts-amqp/dist/interfaces/Queue';
import ChannelN from 'ts-amqp/dist/classes/ChannelN';
import Registry from './registry';

export interface IPeanarJobDefinitionInput {
  queue: string;
  name?: string;
  routingKey?: string;
  exchange?: string;
  replyTo?: string;
  handler: (...args: any[]) => Promise<any>;

  expires?: number;

  retry_exchange?: string;
  error_exchange?: string;
  max_retries?: number;
  retry_delay?: number;
}

export interface IPeanarJobDefinition {
  name: string;
  queue: string;
  handler: (...args: any[]) => Promise<any>;

  routingKey: string;
  exchange?: string;
  replyTo?: string;

  expires?: number;

  retry_exchange?: string;
  error_exchange?: string;
  max_retries?: number;
  retry_delay?: number;
}

export interface IPeanarRequest {
  id: string;
  name: string;
  args: any[];
  attempt: number;
  correlationId?: string;
  deliveryTag?: bigint;
}

export interface IPeanarJob extends IPeanarJobDefinition, IPeanarRequest {
  deliveryTag: bigint;
}

export interface IPeanarResponse {
  id: string;
  name: string;
  status: 'SUCCESS' | 'FAILURE';
  error?: unknown;
  result?: unknown;
}

export interface IPeanarOptions {
  connection?: IConnectionParams;
  jobClass?: typeof PeanarJob;
  logger?(...args: any[]): any;
}

interface IWorkerOptions {
  queues?: string[];
  concurrency?: number;
  prefetch?: number;
}

enum EAppState {
  RUNNING = 'RUNNING',
  CLOSING = 'CLOSING',
  CLOSED = 'CLOSED'
}

export default class PeanarApp {
  public registry = new Registry;

  public log: (...args: any[]) => any;
  public broker: Broker;
  public jobClass: typeof PeanarJob;

  protected consumers: Map<string, Consumer[]> = new Map;
  protected workers: Map<string, Worker[]> = new Map;
  protected jobs: Map<string, (...args: unknown[]) => Promise<unknown>> = new Map;

  protected _connectionPromise?: Promise<ChannelN>;

  public state: EAppState = EAppState.RUNNING;

  constructor(options: IPeanarOptions = {}) {
    this.broker = new Broker(this, options.connection);
    this.jobClass = options.jobClass || PeanarJob;
    this.log = options.logger || console.log.bind(console);
  }

  protected async _ensureConnected() {
    debug('Peanar: ensureConnected()');

    if (this.broker.channel) return this.broker.channel;
    if (!this._connectionPromise) {
      this._connectionPromise = this.broker.connect();
    }
    return this._connectionPromise;
  }

  protected async _shutdown(timeout?: number) {
    await Promise.all([...this.consumers.values()].flat().map(c => c.cancel()))
    await Promise.all([...this.workers.values()].flat().map(w => w.shutdown(timeout)))

    await this.broker.shutdown();
  }

  public async shutdown(timeout?: number) {
    this.log('Peanar: shutdown()');

    this.state = EAppState.CLOSING;
    await this._shutdown(timeout);
    this.state = EAppState.CLOSED;
  }

  protected _registerWorker(queue: string, worker: Worker) {
    const workers = this.workers.get(queue) || [];
    workers.push(worker);
    this.workers.set(queue, workers);
  }

  protected _registerConsumer(queue: string, consumer: Consumer) {
    const consumers = this.consumers.get(queue) || [];
    consumers.push(consumer);
    this.consumers.set(queue, consumers);
  }

  public call(name: string, args: unknown[]) {
    const enqueue = this.jobs.get(name);

    if (typeof enqueue !== 'function') {
      throw new PeanarInternalError(`Job ${name} is not registered with the app.`);
    }

    return enqueue.apply(this, args);
  }

  public async enqueueJob(def: Omit<IPeanarJobDefinition, 'handler'>, req: IPeanarRequest) {
    debug(`Peanar: enqueueJob(${def.queue}:${def.name}})`);

    const channel = await this._ensureConnected();
    const bindings = [];

    if (def.exchange) {
      await this.broker.declareExchange(def.exchange);

      bindings.push({
        exchange: def.exchange,
        routingKey: def.queue
      });
    }

    if (def.retry_exchange) {
      await this.broker.declareExchange(def.retry_exchange, 'topic');
      await this.broker.declareQueue(def.queue, {
        deadLetterExchange: def.retry_exchange
      }, bindings);
    } else {
      await this.broker.declareQueue(def.queue, {}, bindings);
    }

    const properties: IBasicProperties = {
      correlationId: req.correlationId,
      replyTo: def.replyTo
    };

    if (typeof def.expires === 'number') {
      properties.expiration = def.expires.toString();
    }

    channel.json.write({
      routing_key: def.routingKey,
      exchange: def.exchange,
      properties,
      body: {
        id: req.id,
        name: req.name,
        args: req.args
      }
    });

    return req.id;
  }

  protected async _enqueueJobResponse(job: PeanarJob, result: IWorkerResult) {
    debug('Peanar: _enqueueJobResponse()')

    if (!job.def.replyTo) throw new PeanarInternalError('PeanarApp::_enqueueJobResponse() called with no replyTo defined')

    const channel = await this._ensureConnected()
    await this.broker.declareQueue(job.def.replyTo)

    channel.json.write({
      routing_key: job.def.replyTo,
      exchange: '',
      properties: {
        correlationId: job.correlationId || job.id
      },
      body: this._prepareJobResponse(job, result)
    });
  }

  protected _prepareJobRequest(name: string, args: any[]): IPeanarRequest {
    return {
      id: uuid.v4(),
      name,
      args,
      attempt: 1
    };
  }

  protected _prepareJobResponse(job: PeanarJob, result: IWorkerResult): IPeanarResponse {
    const res: IPeanarResponse = {
      id: job.id,
      name: job.name,
      status: result.status,
    };

    if (result.status === 'SUCCESS') {
      res.result = result.result;
    }
    else {
      res.error = result.error;
    }

    return res;
  }

  public job(def: IPeanarJobDefinitionInput) {
    const job_def = this.registry.registerJob(def);
    debug(`Peanar: job('${def.queue}', '${job_def.name}')`);

    const self = this

    function enqueueJob() {
      debug(`Peanar: job.enqueueJobLater('${job_def.name}', ${[...arguments]})`)
      return self.enqueueJob(job_def, self._prepareJobRequest(job_def.name, [...arguments]))
    }

    enqueueJob.rpc = async function() {}

    this.jobs.set(job_def.name, enqueueJob);
    return enqueueJob;
  }

  private async _declareQueue(queue: string) {
    const defs = this.registry.getQueueDefs(queue);

    if (defs) {
      const def = [...defs.values()].find(d => d.retry_exchange);
      const args: IQueueArgs = {};

      if (def) {
        args.deadLetterExchange = def.retry_exchange;
      }

      function hasExchange(d: IPeanarJobDefinition): d is Required<IPeanarJobDefinition> {
        return typeof d.exchange === 'string';
      }

      const defExchange = (d: Required<IPeanarJobDefinition>) => {
        return this.broker.declareExchange(d.exchange, 'direct');
      }

      await Promise.all([...defs.values()].filter(hasExchange).map(defExchange));

      const bindings = [...defs.values()].filter(hasExchange).map(d => ({
        exchange: d.exchange,
        routingKey: d.routingKey
      }));

      await this.broker.declareQueue(queue, args, bindings);
    }
  }

  public pauseQueue(queue: string) {
    const consumers = this.consumers.get(queue);
    if (!consumers) return;

    for (const c of consumers) c.pause();
  }

  public resumeQueue(queue: string) {
    const consumers = this.consumers.get(queue);
    if (!consumers) return;

    for (const c of consumers) c.resume();
  }

  protected async _startWorker(queue: string) {
    const channel = await this._ensureConnected();

    const consumer = await channel.basicConsume(queue);
    const worker = new Worker(this, channel, queue)

    this._registerConsumer(queue, consumer);
    this._registerWorker(queue, worker);

    return consumer
      .pipe(worker)
      .pipe(new Writable({
        objectMode: true,
        write: (result: IWorkerResult, _encoding: string, cb: TransformCallback) => {
          if (result.job.def.replyTo) {
            this._enqueueJobResponse(result.job, result).then(_ => cb(), ex => cb(ex));
          }
          else {
            return cb()
          }
        }
      }));
  }

  public async worker(options: IWorkerOptions) {
    const { queues, concurrency, prefetch = 1 } = options;

    await this._ensureConnected();
    await this.broker.prefetch(prefetch);

    const worker_queues = (Array.isArray(queues) && queues.length > 0)
      ? queues
      : this.registry.queues;

    await Promise.all(worker_queues.map(q => this._declareQueue(q)));

    const queues_to_start = worker_queues.flatMap(q => Array(concurrency).fill(q));

    return Promise.all(queues_to_start.map(q => this._startWorker(q)));
  }
}
