import uuid from 'uuid';
import debugFn from 'debug';
const debug = debugFn('peanar:app');

import { PeanarInternalError } from './exceptions';
import Broker from './broker';
import Worker, { IWorkerResult } from './worker';
import PeanarJob from './job';
import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';
import { Writable, TransformCallback } from 'stream';
import Consumer from 'ts-amqp/dist/classes/Consumer';
import { IBasicProperties } from 'ts-amqp/dist/interfaces/Protocol';
import ChannelN from 'ts-amqp/dist/classes/ChannelN';
import Registry from './registry';
import CloseReason from 'ts-amqp/dist/utils/CloseReason';

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
  poolSize?: number;
  prefetch?: number;
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
    this.broker = new Broker({
      connection: options.connection,
      poolSize: options.poolSize || 5,
      prefetch: options.prefetch || 1
    });

    this.jobClass = options.jobClass || PeanarJob;
    this.log = options.logger || console.log.bind(console);
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

  public async enqueueJobRequest(def: IPeanarJobDefinition, req: IPeanarRequest) {
    debug(`Peanar: enqueueJob(${def.queue}:${def.name}})`);

    const properties: IBasicProperties = {
      correlationId: req.correlationId,
      replyTo: def.replyTo
    };

    if (typeof def.expires === 'number') {
      properties.expiration = def.expires.toString();
    }

    await this.broker.publish({
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

    await this.broker.publish({
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

  protected _createEnqueuer(def: IPeanarJobDefinition) {
    const self = this;
    function enqueueJob(...args: unknown[]): Promise<string> {
      debug(`Peanar: job.enqueueJob('${def.name}')`)
      return self.enqueueJobRequest(def, self._prepareJobRequest(def.name, args))
    }

    enqueueJob.rpc = async (...args: unknown[]) => {};
    return enqueueJob;
  }

  public job(def: IPeanarJobDefinitionInput) {
    const job_def = this.registry.registerJob(def);
    debug(`Peanar: job('${def.queue}', '${job_def.name}')`);

    const enqueueJob = this._createEnqueuer(job_def);

    this.jobs.set(job_def.name, enqueueJob);
    return enqueueJob;
  }

  public async declareAmqResources() {
    await this.broker.queues(this.registry.queues);
    await this.broker.exchanges(this.registry.exchanges);
    await this.broker.bindings(this.registry.bindings);
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

  protected async _startWorker(queue: string, consumer: Consumer) {
    const worker = new Worker(this, consumer.channel, queue);

    consumer.channel.once('channelClose', async (reason: CloseReason) => {
      consumer.unpipe(worker);
      const queue_consumers = this.consumers.get(queue) || [];
      queue_consumers.splice(queue_consumers.indexOf(consumer), 1);

      if (reason.reply_code >= 400) {
        const new_consumer = await this.broker.consume(queue);
        this._registerConsumer(queue, new_consumer);
        consumer.pipe(worker);
      }
    });

    this._registerConsumer(queue, consumer);
    this._registerWorker(queue, worker);

    return consumer
      .pipe(worker)
      .pipe(new Writable({
        objectMode: true,
        write: (result: IWorkerResult, _encoding: string, cb: TransformCallback) => {
          if (result.status === 'FAILURE') {
            this.log(result.error);
          }

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

    const worker_queues = (Array.isArray(queues) && queues.length > 0)
      ? queues
      : this.registry.workerQueues;

    const queues_to_start = [...worker_queues].flatMap(q => Array(concurrency).fill(q));

    return Promise.all((await this.broker.consumeOver(queues_to_start)).map(async p => {
      const { queue, consumer} = await p;

      return this._startWorker(queue, consumer);
    }));
  }
}
